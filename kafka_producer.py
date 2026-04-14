"""
kafka_producer.py
=================
Receiver UDP ultra-rapide -> Kafka Producer.

Ce process remplace receiving_messages() de logger.py.
Il ne fait AUCUN parsing : il envoie les bytes bruts + packet_time dans Kafka.
Le parsing est delegue aux consumers (kafka_consumer.py).

Format du message Kafka :
    struct.pack("!d", packet_time)  [8 bytes, big-endian]
    + received_data                 [N bytes, bytes bruts UDP]

Partitionnement : pdu_type % NUM_PARTITIONS
    -> tous les EntityStatePdu vont dans la meme partition
    -> coherence du cache entity_locs_cache dans chaque consumer
"""

import datetime
import logging
import logging.handlers
import os
import socket
import struct
import time
from multiprocessing import Queue, Event
from typing import Optional

from confluent_kafka import Producer, KafkaException

import kafka_config as cfg

# ---------------------------------------------------------------------------
# Logger (ecrit dans la log_queue du processus parent)
# ---------------------------------------------------------------------------
log = logging.getLogger("kafka_producer")


# ---------------------------------------------------------------------------
# Callback de livraison (appele par producer.poll() dans la boucle principale)
# ---------------------------------------------------------------------------
def _delivery_report(err, msg) -> None:
    """
    Callback appele par confluent_kafka pour chaque message apres tentative d'envoi.
    En cas d'erreur, on log mais on ne bloque PAS la reception UDP.
    Les erreurs transitoires (broker indisponible) sont gerees en amont par les retries.
    """
    if err is not None:
        log.error(
            "Echec livraison Kafka | topic=%s partition=%d offset=%s | err=%s",
            msg.topic(), msg.partition(), msg.offset(), err
        )
    # En cas de succes, on ne log pas (trop verbeux a 20k msg/sec)


# ---------------------------------------------------------------------------
# Fonction principale du process producer
# ---------------------------------------------------------------------------
def run_producer(
    stop_event: Event,
    log_queue: Queue,
    start_time: float,
) -> None:
    """
    Point d'entree du process UDP receiver + Kafka producer.

    Parametres
    ----------
    stop_event : multiprocessing.Event
        Signale par kafka_main.py pour arreter proprement le process.
    log_queue : multiprocessing.Queue
        Queue de logging centralisee (QueueHandler -> QueueListener dans main).
    start_time : float
        Timestamp Unix de demarrage du pipeline (pour calculer packet_time relatif).
    """
    # --- Initialisation du logger worker ---
    _configure_worker_logger(log_queue)
    log.info("Producer demarre -- PID %d", os.getpid())
    print(f"kafka_producer PID={os.getpid()}")

    # --- Initialisation du producer Kafka ---
    producer = _create_producer()

    # --- Initialisation du socket UDP ---
    sock = _create_udp_socket(cfg.PORT, cfg.MESSAGE_LENGTH)

    # --- Compteurs pour les stats ---
    count_sent: int     = 0   # messages produits dans Kafka avec succes
    count_errors: int   = 0   # messages pour lesquels produce() a leve une exception
    count_buffer_err: int = 0  # BufferError (queue interne Kafka saturee)
    last_stat_time: float = time.monotonic()

    log.info(
        "En ecoute UDP sur port %d | topic=%s | %d partitions",
        cfg.PORT, cfg.KAFKA_TOPIC, cfg.KAFKA_NUM_PARTITIONS
    )

    try:
        while not stop_event.is_set():
            try:
                # ----------------------------------------------------------------
                # 1. Reception UDP
                #    recvfrom() est bloquant jusqu'a l'arrivee d'un datagramme.
                #    MESSAGE_LENGTH est le buffer max (262144 bytes par defaut).
                # ----------------------------------------------------------------
                received_data, _addr = sock.recvfrom(cfg.MESSAGE_LENGTH)

                # ----------------------------------------------------------------
                # 2. Calcul du packet_time relatif
                #    (secondes ecoulees depuis le demarrage du pipeline)
                # ----------------------------------------------------------------
                packet_time: float = datetime.datetime.now().timestamp() - start_time

                # ----------------------------------------------------------------
                # 3. Construction du message Kafka
                #    header 8 bytes (packet_time big-endian) + PDU brut
                # ----------------------------------------------------------------
                kafka_value: bytes = struct.pack("!d", packet_time) + received_data

                # ----------------------------------------------------------------
                # 4. Determination de la partition
                #    Le 3eme byte du PDU DIS = pduType.
                #    On regroupe les memes types ensemble -> coherence des caches
                #    dans les consumers (notamment entity_locs_cache).
                # ----------------------------------------------------------------
                pdu_type: int = received_data[2]
                partition: int = pdu_type % cfg.KAFKA_NUM_PARTITIONS

                # ----------------------------------------------------------------
                # 5. Envoi dans Kafka (non-bloquant)
                #    produce() ajoute dans le buffer interne du producer.
                #    L'envoi reseau reel se fait via poll() ou flush().
                # ----------------------------------------------------------------
                _produce_with_retry(producer, kafka_value, partition)
                count_sent += 1

                # ----------------------------------------------------------------
                # 6. Traitement des callbacks (livraisons, erreurs)
                #    poll(0) = non-bloquant : traite tous les callbacks en attente.
                # ----------------------------------------------------------------
                producer.poll(0)

            except IndexError:
                # PDU trop court pour avoir un byte[2] -- on ignore
                log.warning("PDU trop court recu (%d bytes) -- ignore", len(received_data))
                continue
            except OSError as exc:
                # Erreur socket (ex : port deja utilise, interface down)
                log.error("Erreur socket UDP : %s", exc)
                count_errors += 1
                continue

            # ----------------------------------------------------------------
            # 7. Stats toutes les 10 secondes
            # ----------------------------------------------------------------
            now = time.monotonic()
            if now - last_stat_time >= 10.0:
                elapsed = now - last_stat_time
                rate = count_sent / elapsed if elapsed > 0 else 0
                log.info(
                    "Producer stats | envoyes=%d | rate=%.0f msg/s | "
                    "erreurs=%d | buffer_errors=%d",
                    count_sent, rate, count_errors, count_buffer_err
                )
                print(
                    f"[producer] envoyes={count_sent} | "
                    f"rate={rate:.0f} msg/s | "
                    f"erreurs={count_errors}"
                )
                # Reinitialise les compteurs pour la prochaine fenetre de stats
                count_sent = 0
                count_errors = 0
                count_buffer_err = 0
                last_stat_time = now

    except KeyboardInterrupt:
        log.info("Producer interrompu par KeyboardInterrupt")

    finally:
        # ----------------------------------------------------------------
        # Shutdown propre :
        # flush() attend que tous les messages en buffer soient livres
        # (ou que le timeout soit atteint).
        # ----------------------------------------------------------------
        log.info("Flush du producer Kafka en cours...")
        remaining = producer.flush(timeout=30)
        if remaining > 0:
            log.warning(
                "%d messages n'ont pas pu etre livres avant le timeout flush", remaining
            )
        sock.close()
        log.info(
            "Producer arrete proprement | total envoye=%d | erreurs=%d",
            count_sent, count_errors
        )
        print(f"[producer] arrete -- {count_sent} msg envoyes, {count_errors} erreurs")


# ---------------------------------------------------------------------------
# Helpers internes
# ---------------------------------------------------------------------------

def _create_producer() -> Producer:
    """
    Cree et retourne un Producer confluent_kafka configure.
    Leve KafkaException si la connexion initiale au broker echoue.
    """
    try:
        producer = Producer(cfg.KAFKA_PRODUCER_CONFIG)
        log.info("Producer Kafka initialise | bootstrap=%s", cfg.KAFKA_BOOTSTRAP)
        return producer
    except KafkaException as exc:
        log.critical("Impossible de creer le Producer Kafka : %s", exc)
        raise


def _create_udp_socket(port: int, message_length: int) -> socket.socket:
    """
    Cree un socket UDP avec un buffer de reception de 128 MB.
    Meme configuration que receiving_messages() dans logger.py original.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Buffer OS de 128 MB pour absorber les bursts sans perte au niveau kernel
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 128 * 1024 * 1024)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # Timeout non-bloquant pour verifier stop_event regulierement
    sock.settimeout(1.0)

    sock.bind(("", port))
    log.info("Socket UDP lie sur le port %d | buffer=128MB", port)
    return sock


def _produce_with_retry(
    producer: Producer,
    value: bytes,
    partition: int,
    max_retries: int = 3,
) -> None:
    """
    Tente d'envoyer un message dans Kafka avec retry en cas de BufferError.

    BufferError survient quand le buffer interne du producer est sature
    (queue.buffering.max.messages atteint). On appelle poll() pour liberer
    de la place avant de reessayer.

    Parametres
    ----------
    producer  : Producer confluent_kafka
    value     : bytes -- payload du message (packet_time + pdu_raw)
    partition : int   -- partition cible
    max_retries : int -- nombre de tentatives avant abandon
    """
    for attempt in range(max_retries):
        try:
            producer.produce(
                topic=cfg.KAFKA_TOPIC,
                value=value,
                partition=partition,
                on_delivery=_delivery_report,
            )
            return  # succes

        except BufferError:
            # Buffer interne sature -> on traite les callbacks en attente
            # pour liberer de l'espace, puis on reessaie
            log.debug(
                "BufferError Kafka (tentative %d/%d) -- poll() pour liberer le buffer",
                attempt + 1, max_retries
            )
            producer.poll(1)  # bloquant 1 seconde max

    # Si on arrive ici, toutes les tentatives ont echoue
    log.error(
        "Message abandonne apres %d tentatives (BufferError persistant)",
        max_retries
    )


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
