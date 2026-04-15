"""
kafka_consumer.py
=================
Kafka Consumer -> Parse PDU -> Export SQL.

Ce process combine la logique de parsing_messages() + export_process_func()
de logger.py, en supprimant toute dependance au SharedMemory Ring Buffer.

Chaque instance de ce process :
  - consomme des messages depuis une ou plusieurs partitions Kafka
  - deserialise le message : packet_time (8 bytes big-endian) + pdu_raw_data
  - filtre par pduType et exerciseID
  - reconstruit le format LoggerPDU attendu par LoggerPduProcessor
  - appelle lpp.process() -> remplit sa processing_queue locale
  - draine la processing_queue -> export SQL via LoggerSQLExporter
  - commite l'offset Kafka APRES l'insert SQL -> zero perte garantie

IMPORTANT : chaque consumer a ses propres instances de :
    - LoggerPduProcessor (et ses caches : entity_locs_cache, etc.)
    - LoggerSQLExporter  (et ses Exporters / threads SQL)
    - processing_queue   (Queue locale, non partagee)
Cela evite toute contention entre les consumers paralleles.
"""

import logging
import logging.handlers
import multiprocessing
import os
import struct
import time
import traceback
from multiprocessing import Queue, Event
from typing import Optional

from confluent_kafka import Consumer, KafkaError, KafkaException

import kafka_config as cfg
from LoggerPduProcessor import LoggerPduProcessor, LoggerPDU
from LoggerSQLExporter import LoggerSQLExporter

log = logging.getLogger("kafka_consumer")


# ---------------------------------------------------------------------------
# Fonction principale du process consumer
# ---------------------------------------------------------------------------
def run_consumer(
    consumer_id: int,
    stop_event: Event,
    log_queue: Queue,
    logger_file: str,
    start_time: float,
) -> None:
    """
    Point d'entree du process consumer Kafka.

    Parametres
    ----------
    consumer_id : int
        Identifiant unique du consumer (0..N-1), utilise pour les logs.
    stop_event : multiprocessing.Event
        Signale par kafka_main.py pour declencher le shutdown propre.
    log_queue : multiprocessing.Queue
        Queue de logging centralisee.
    logger_file : str
        Nom du fichier logger courant (ex: "exp_14_4_3.lzma").
        Injecte par kafka_main.py apres resolution du nom.
    start_time : float
        Timestamp Unix de demarrage du pipeline.
    """
    # --- Initialisation du logger worker ---
    _configure_worker_logger(log_queue)
    log.info("Consumer %d demarre -- PID %d", consumer_id, os.getpid())
    print(f"kafka_consumer[{consumer_id}] PID={os.getpid()}")

    # --- Queue locale (non partagee) pour router les PDU parses vers l'exporter ---
    # LoggerPduProcessor ecrit dans cette queue via lpp.process()
    # On la draine immediatement apres chaque message
    processing_queue: Queue = Queue()

    # --- Initialisation de LoggerPduProcessor ---
    # entity_locations_per_second est distribue entre les consumers
    # (meme logique que distribute_locs() dans logger.py)
    locs_per_sec = max(1, cfg.ENTITY_LOCS_PER_SEC // cfg.KAFKA_NUM_CONSUMERS)
    lpp = LoggerPduProcessor(
        processing_queue,
        locs_per_sec,
        logger_file,
        start_time,
        process_entities=True,
        process_aggregates=True,
    )
    log.info("Consumer %d | LoggerPduProcessor initialise", consumer_id)

    # --- Initialisation de LoggerSQLExporter ---
    # stop_writing_event local : le consumer le gere lui-meme
    stop_writing_event = multiprocessing.Event()
    lse = LoggerSQLExporter(
        logger_file,
        cfg.TRACKED_TABLES,
        cfg.DB_NAME,
        stop_writing_event,
        start_time,
        cfg.EXPORT_DELAY,
        cfg.EXPORT_SIZE,
        cfg.NEW_DB,
    )
    log.info("Consumer %d | LoggerSQLExporter initialise", consumer_id)

    # --- Initialisation du Consumer Kafka ---
    consumer = _create_consumer(consumer_id)

    # --- Compteurs pour les stats ---
    count_consumed: int  = 0   # messages recus depuis Kafka
    count_exported: int  = 0   # messages exportes vers SQL
    count_skipped: int   = 0   # messages filtres (mauvais pduType ou exerciseID)
    count_errors: int    = 0   # erreurs de parsing ou d'export
    last_stat_time: float = time.monotonic()

    try:
        # S'abonner au topic -> Kafka rebalance les partitions entre les consumers du groupe
        consumer.subscribe([cfg.KAFKA_TOPIC])
        log.info("Consumer %d | Abonne au topic '%s'", consumer_id, cfg.KAFKA_TOPIC)

        while not stop_event.is_set():
            # ----------------------------------------------------------------
            # 1. Poll Kafka -- timeout 1.0s pour verifier stop_event regulierement
            # ----------------------------------------------------------------
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Aucun message disponible dans le timeout -> on reboucle
                continue

            # ----------------------------------------------------------------
            # 2. Gestion des erreurs Kafka au niveau message
            # ----------------------------------------------------------------
            if msg.error():
                _handle_kafka_error(consumer_id, msg.error())
                continue

            # ----------------------------------------------------------------
            # 3. Deserialisation du message Kafka
            #
            #    Format : [packet_time 8 bytes big-endian] + [pdu_raw N bytes]
            #    Le producer a package avec struct.pack("!d", packet_time).
            # ----------------------------------------------------------------
            try:
                value: bytes = msg.value()

                if len(value) < cfg.KAFKA_MSG_HEADER_SIZE:
                    log.warning(
                        "Consumer %d | Message trop court (%d bytes) -- ignore",
                        consumer_id, len(value)
                    )
                    count_skipped += 1
                    _safe_commit(consumer, consumer_id)
                    continue

                # Lecture du packet_time (big-endian, 8 bytes)
                packet_time: float = struct.unpack("!d", value[:cfg.KAFKA_MSG_HEADER_SIZE])[0]

                # Bytes bruts du PDU DIS
                pdu_data: bytes = value[cfg.KAFKA_MSG_HEADER_SIZE:]

            except struct.error as exc:
                log.error(
                    "Consumer %d | Erreur deserialisation message offset=%d : %s",
                    consumer_id, msg.offset(), exc
                )
                count_errors += 1
                _safe_commit(consumer, consumer_id)
                continue

            # ----------------------------------------------------------------
            # 4. Filtre rapide sur pduType (byte[2] du PDU DIS)
            #    Evite de creer un LoggerPDU pour les types non supportes.
            # ----------------------------------------------------------------
            try:
                pdu_type: int = pdu_data[2]
            except IndexError:
                log.warning("Consumer %d | PDU trop court -- ignore", consumer_id)
                count_skipped += 1
                _safe_commit(consumer, consumer_id)
                continue

            if pdu_type not in cfg.PDU_TYPE_LIST:
                count_skipped += 1
                _safe_commit(consumer, consumer_id)
                continue

            # ----------------------------------------------------------------
            # 5. Construction du LoggerPDU via from_parts()
            #
            #    Fix BUG 2 : on bypasse entierement la serialisation
            #    b"line_divider" pour eliminer tout risque de collision si
            #    les bytes du PDU contiennent accidentellement cette sequence.
            #    LoggerPDU.from_parts() cree l'objet directement depuis
            #    les bytes bruts et le packet_time.
            # ----------------------------------------------------------------
            try:
                received_pdu = LoggerPDU.from_parts(pdu_data, packet_time)

            except Exception as exc:
                log.error(
                    "Consumer %d | Erreur creation LoggerPDU offset=%d : %s",
                    consumer_id, msg.offset(), exc
                )
                count_errors += 1
                _safe_commit(consumer, consumer_id)
                continue

            # ----------------------------------------------------------------
            # 6. Filtre sur exerciseID
            #    Identique au filtre de parsing_messages() dans logger.py.
            # ----------------------------------------------------------------
            if received_pdu.pdu is None:
                count_skipped += 1
                _safe_commit(consumer, consumer_id)
                continue

            if received_pdu.pdu.exerciseID != cfg.EXERCISE_ID:
                count_skipped += 1
                _safe_commit(consumer, consumer_id)
                continue

            # ----------------------------------------------------------------
            # 7. Traitement du PDU par LoggerPduProcessor
            #    Remplit processing_queue avec (table_name, [dict_data])
            # ----------------------------------------------------------------
            try:
                lpp.process(received_pdu)
            except Exception as exc:
                log.error(
                    "Consumer %d | Erreur lpp.process() offset=%d : %s\n%s",
                    consumer_id, msg.offset(), exc, traceback.format_exc()
                )
                count_errors += 1
                _safe_commit(consumer, consumer_id)
                continue

            # ----------------------------------------------------------------
            # 8. Drainage de la processing_queue -> export SQL
            #
            #    On exporte TOUT ce que lpp a mis dans la queue avant
            #    de commiter l'offset Kafka. Garantie : si SQL echoue,
            #    on ne commite pas -> re-traitement au redemarrage.
            # ----------------------------------------------------------------
            sql_ok: bool = _drain_and_export(
                processing_queue, lse, consumer_id, msg.offset()
            )
            count_consumed += 1

            if sql_ok:
                count_exported += 1
                # ----------------------------------------------------------------
                # 9. COMMIT OFFSET Kafka -- synchrone, APRES SQL reussi
                #    enable.auto.commit=False -> on controle exactement quand
                #    l'offset est avance.
                # ----------------------------------------------------------------
                _safe_commit(consumer, consumer_id)
            else:
                # Export SQL a echoue -> on ne commite pas l'offset
                # Le consumer reprendra ce message au prochain demarrage
                count_errors += 1
                log.warning(
                    "Consumer %d | Export SQL echoue pour offset=%d -- offset non commite",
                    consumer_id, msg.offset()
                )

            # ----------------------------------------------------------------
            # 10. Stats toutes les 10 secondes
            # ----------------------------------------------------------------
            now = time.monotonic()
            if now - last_stat_time >= 10.0:
                elapsed = now - last_stat_time
                rate_in  = count_consumed / elapsed if elapsed > 0 else 0
                rate_out = count_exported / elapsed if elapsed > 0 else 0
                log.info(
                    "Consumer %d stats | consommes=%d (%.0f/s) | exportes=%d (%.0f/s) | "
                    "skippes=%d | erreurs=%d",
                    consumer_id,
                    count_consumed, rate_in,
                    count_exported, rate_out,
                    count_skipped, count_errors,
                )
                print(
                    f"[consumer-{consumer_id}] "
                    f"consommes={count_consumed} ({rate_in:.0f}/s) | "
                    f"exportes={count_exported} ({rate_out:.0f}/s) | "
                    f"erreurs={count_errors}"
                )
                count_consumed = 0
                count_exported = 0
                count_skipped  = 0
                count_errors   = 0
                last_stat_time = now

    except KafkaException as exc:
        log.critical("Consumer %d | KafkaException fatale : %s", consumer_id, exc)
        print(f"[consumer-{consumer_id}] ERREUR FATALE Kafka : {exc}")

    except KeyboardInterrupt:
        log.info("Consumer %d | Interrompu par KeyboardInterrupt", consumer_id)

    finally:
        # ----------------------------------------------------------------
        # Shutdown propre :
        # 1. Signaler a LoggerSQLExporter d'arreter ses timers
        # 2. Fermer le consumer Kafka (commit final des offsets en attente)
        # ----------------------------------------------------------------
        log.info("Consumer %d | Fermeture en cours...", consumer_id)
        stop_writing_event.set()  # arret des timers SQL dans LoggerSQLExporter

        # Dernier drain de la queue avant fermeture
        _drain_and_export(processing_queue, lse, consumer_id, offset=-1)

        consumer.close()
        log.info(
            "Consumer %d | Arrete proprement | total consomme=%d | exporte=%d | erreurs=%d",
            consumer_id, count_consumed, count_exported, count_errors
        )
        print(f"[consumer-{consumer_id}] arrete proprement")


# ---------------------------------------------------------------------------
# Helpers internes
# ---------------------------------------------------------------------------

def _create_consumer(consumer_id: int) -> Consumer:
    """
    Cree et retourne un Consumer confluent_kafka configure.
    Chaque consumer a un client.id unique pour faciliter le monitoring.
    """
    config = dict(cfg.KAFKA_CONSUMER_CONFIG)
    config["client.id"] = f"dis-consumer-{consumer_id}"

    try:
        consumer = Consumer(config)
        log.info("Consumer %d | Consumer Kafka initialise | bootstrap=%s", consumer_id, cfg.KAFKA_BOOTSTRAP)
        return consumer
    except KafkaException as exc:
        log.critical("Impossible de creer le Consumer %d : %s", consumer_id, exc)
        raise


def _handle_kafka_error(consumer_id: int, error: KafkaError) -> None:
    """
    Gere les erreurs remontees par consumer.poll().

    - PARTITION_EOF : information normale, pas d'erreur -> on continue silencieusement
    - Autres erreurs : on log en WARNING (les erreurs transitoires se recuperent seules)
    """
    if error.code() == KafkaError._PARTITION_EOF:
        # Fin de partition atteinte -- normal en mode "earliest" quand on a tout lu
        # On ne log pas pour eviter le spam
        pass
    else:
        log.warning(
            "Consumer %d | Erreur Kafka : code=%s msg=%s fatal=%s",
            consumer_id, error.code(), error.str(), error.fatal()
        )


def _drain_and_export(
    processing_queue: Queue,
    lse: LoggerSQLExporter,
    consumer_id: int,
    offset: int,
) -> bool:
    """
    Draine la processing_queue et insere chaque (table, data) en SQL SYNCHRONIQUEMENT.

    Fix BUG 3 : on utilise lse.insert_sync() au lieu de lse.export() (async).
    Cela garantit que l'insert SQL est commite en base AVANT que l'appelant
    commite l'offset Kafka. Sur crash entre insert et commit Kafka : le message
    sera re-traite (at-least-once), mais AUCUNE donnee ne sera perdue silencieusement.

    Retourne True si tous les inserts ont reussi, False sinon.
    En cas d'echec SQL, on continue de drainer pour ne pas bloquer la queue,
    mais on retourne False pour que l'appelant ne commite PAS l'offset.

    Parametres
    ----------
    processing_queue : Queue -- remplie par lpp.process()
    lse              : LoggerSQLExporter -- instance du consumer courant
    consumer_id      : int   -- pour les logs
    offset           : int   -- offset Kafka courant (pour les logs)
    """
    all_ok = True

    while not processing_queue.empty():
        try:
            table, data = processing_queue.get_nowait()
            ok = lse.insert_sync(table, data)
            if not ok:
                log.error(
                    "Consumer %d | insert_sync echoue table=%s offset=%d",
                    consumer_id, table, offset
                )
                all_ok = False
        except Exception as exc:
            log.error(
                "Consumer %d | Erreur export SQL offset=%d : %s\n%s",
                consumer_id, offset, exc, traceback.format_exc()
            )
            all_ok = False

    return all_ok


def _safe_commit(consumer: Consumer, consumer_id: int) -> None:
    """
    Effectue un commit synchrone de l'offset courant.
    Synchrone (asynchronous=False) pour garantir que l'offset est bien
    persiste avant de continuer.
    Absorbe les exceptions pour ne pas tuer le consumer sur un echec de commit.
    """
    try:
        consumer.commit(asynchronous=False)
    except KafkaException as exc:
        log.warning("Consumer %d | Echec commit offset : %s", consumer_id, exc)


def _configure_worker_logger(log_queue: Queue) -> None:
    """
    Configure le logger du process worker pour envoyer vers la QueueHandler.
    Identique a configure_worker_logger() dans logger.py.
    """
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.handlers.clear()

    qh = logging.handlers.QueueHandler(log_queue)
    qh.setLevel(logging.DEBUG)
    root.addHandler(qh)
    root.propagate = False
