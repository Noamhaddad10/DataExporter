"""
kafka_producer.py
=================
Receiver UDP ultra-rapide → Kafka Producer.

Ce process remplace receiving_messages() de logger.py.
Il ne fait AUCUN parsing : il envoie les bytes bruts + packet_time dans Kafka.
Le parsing est délégué aux consumers (kafka_consumer.py).

Format du message Kafka :
    struct.pack("!d", packet_time)  [8 bytes, big-endian]
    + received_data                 [N bytes, bytes bruts UDP]

Partitionnement : pdu_type % NUM_PARTITIONS
    → tous les EntityStatePdu vont dans la même partition
    → cohérence du cache entity_locs_cache dans chaque consumer
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
# Logger (écrit dans la log_queue du processus parent)
# ---------------------------------------------------------------------------
log = logging.getLogger("kafka_producer")


# ---------------------------------------------------------------------------
# Callback de livraison (appelé par producer.poll() dans la boucle principale)
# ---------------------------------------------------------------------------
def _delivery_report(err, msg) -> None:
    """
    Callback appelé par confluent_kafka pour chaque message après tentative d'envoi.
    En cas d'erreur, on log mais on ne bloque PAS la réception UDP.
    Les erreurs transitoires (broker indisponible) sont gérées en amont par les retries.
    """
    if err is not None:
        log.error(
            "Echec livraison Kafka | topic=%s partition=%d offset=%s | err=%s",
            msg.topic(), msg.partition(), msg.offset(), err
        )
    # En cas de succès, on ne log pas (trop verbeux à 20k msg/sec)


# ---------------------------------------------------------------------------
# Fonction principale du process producer
# ---------------------------------------------------------------------------
def run_producer(
    stop_event: Event,
    log_queue: Queue,
    start_time: float,
) -> None:
    """
    Point d'entrée du process UDP receiver + Kafka producer.

    Paramètres
    ----------
    stop_event : multiprocessing.Event
        Signalé par kafka_main.py pour arrêter proprement le process.
    log_queue : multiprocessing.Queue
        Queue de logging centralisée (QueueHandler → QueueListener dans main).
    start_time : float
        Timestamp Unix de démarrage du pipeline (pour calculer packet_time relatif).
    """
    # --- Initialisation du logger worker ---
    _configure_worker_logger(log_queue)
    log.info("Producer démarré — PID %d", os.getpid())
    print(f"kafka_producer PID={os.getpid()}")

    # --- Initialisation du producer Kafka ---
    producer = _create_producer()

    # --- Initialisation du socket UDP ---
    sock = _create_udp_socket(cfg.PORT, cfg.MESSAGE_LENGTH)

    # --- Compteurs pour les stats ---
    count_sent: int     = 0   # messages produits dans Kafka avec succès
    count_errors: int   = 0   # messages pour lesquels produce() a levé une exception
    count_buffer_err: int = 0  # BufferError (queue interne Kafka saturée)
    last_stat_time: float = time.monotonic()

    log.info(
        "En écoute UDP sur port %d | topic=%s | %d partitions",
        cfg.PORT, cfg.KAFKA_TOPIC, cfg.KAFKA_NUM_PARTITIONS
    )

    try:
        while not stop_event.is_set():
            try:
                # ----------------------------------------------------------------
                # 1. Réception UDP
                #    recvfrom() est bloquant jusqu'à l'arrivée d'un datagramme.
                #    MESSAGE_LENGTH est le buffer max (262144 bytes par défaut).
                # ----------------------------------------------------------------
                received_data, _addr = sock.recvfrom(cfg.MESSAGE_LENGTH)

                # ----------------------------------------------------------------
                # 2. Calcul du packet_time relatif
                #    (secondes écoulées depuis le démarrage du pipeline)
                # ----------------------------------------------------------------
                packet_time: float = datetime.datetime.now().timestamp() - start_time

                # ----------------------------------------------------------------
                # 3. Construction du message Kafka
                #    header 8 bytes (packet_time big-endian) + PDU brut
                # ----------------------------------------------------------------
                kafka_value: bytes = struct.pack("!d", packet_time) + received_data

                # ----------------------------------------------------------------
                # 4. Détermination de la partition
                #    Le 3ème byte du PDU DIS = pduType.
                #    On regroupe les mêmes types ensemble → cohérence des caches
                #    dans les consumers (notamment entity_locs_cache).
                # ----------------------------------------------------------------
                pdu_type: int = received_data[2]
                partition: int = pdu_type % cfg.KAFKA_NUM_PARTITIONS

                # ----------------------------------------------------------------
                # 5. Envoi dans Kafka (non-bloquant)
                #    produce() ajoute dans le buffer interne du producer.
                #    L'envoi réseau réel se fait via poll() ou flush().
                # ----------------------------------------------------------------
                _produce_with_retry(producer, kafka_value, partition)
                count_sent += 1

                # ----------------------------------------------------------------
                # 6. Traitement des callbacks (livraisons, erreurs)
                #    poll(0) = non-bloquant : traite tous les callbacks en attente.
                # ----------------------------------------------------------------
                producer.poll(0)

            except IndexError:
                # PDU trop court pour avoir un byte[2] — on ignore
                log.warning("PDU trop court reçu (%d bytes) — ignoré", len(received_data))
                continue
            except OSError as exc:
                # Erreur socket (ex : port déjà utilisé, interface down)
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
                    "Producer stats | envoyés=%d | rate=%.0f msg/s | "
                    "erreurs=%d | buffer_errors=%d",
                    count_sent, rate, count_errors, count_buffer_err
                )
                print(
                    f"[producer] envoyés={count_sent} | "
                    f"rate={rate:.0f} msg/s | "
                    f"erreurs={count_errors}"
                )
                # Réinitialise les compteurs pour la prochaine fenêtre de stats
                count_sent = 0
                count_errors = 0
                count_buffer_err = 0
                last_stat_time = now

    except KeyboardInterrupt:
        log.info("Producer interrompu par KeyboardInterrupt")

    finally:
        # ----------------------------------------------------------------
        # Shutdown propre :
        # flush() attend que tous les messages en buffer soient livrés
        # (ou que le timeout soit atteint).
        # ----------------------------------------------------------------
        log.info("Flush du producer Kafka en cours...")
        remaining = producer.flush(timeout=30)
        if remaining > 0:
            log.warning(
                "%d messages n'ont pas pu être livrés avant le timeout flush", remaining
            )
        sock.close()
        log.info(
            "Producer arrêté proprement | total envoyé=%d | erreurs=%d",
            count_sent, count_errors
        )
        print(f"[producer] arrêté — {count_sent} msg envoyés, {count_errors} erreurs")


# ---------------------------------------------------------------------------
# Helpers internes
# ---------------------------------------------------------------------------

def _create_producer() -> Producer:
    """
    Crée et retourne un Producer confluent_kafka configuré.
    Lève KafkaException si la connexion initiale au broker échoue.
    """
    try:
        producer = Producer(cfg.KAFKA_PRODUCER_CONFIG)
        log.info("Producer Kafka initialisé | bootstrap=%s", cfg.KAFKA_BOOTSTRAP)
        return producer
    except KafkaException as exc:
        log.critical("Impossible de créer le Producer Kafka : %s", exc)
        raise


def _create_udp_socket(port: int, message_length: int) -> socket.socket:
    """
    Crée un socket UDP avec un buffer de réception de 128 MB.
    Même configuration que receiving_messages() dans logger.py original.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Buffer OS de 128 MB pour absorber les bursts sans perte au niveau kernel
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 128 * 1024 * 1024)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # Timeout non-bloquant pour vérifier stop_event régulièrement
    sock.settimeout(1.0)

    sock.bind(("", port))
    log.info("Socket UDP lié sur le port %d | buffer=128MB", port)
    return sock


def _produce_with_retry(
    producer: Producer,
    value: bytes,
    partition: int,
    max_retries: int = 3,
) -> None:
    """
    Tente d'envoyer un message dans Kafka avec retry en cas de BufferError.

    BufferError survient quand le buffer interne du producer est saturé
    (queue.buffering.max.messages atteint). On appelle poll() pour libérer
    de la place avant de réessayer.

    Paramètres
    ----------
    producer  : Producer confluent_kafka
    value     : bytes — payload du message (packet_time + pdu_raw)
    partition : int   — partition cible
    max_retries : int — nombre de tentatives avant abandon
    """
    for attempt in range(max_retries):
        try:
            producer.produce(
                topic=cfg.KAFKA_TOPIC,
                value=value,
                partition=partition,
                on_delivery=_delivery_report,
            )
            return  # succès

        except BufferError:
            # Buffer interne saturé → on traite les callbacks en attente
            # pour libérer de l'espace, puis on réessaie
            log.debug(
                "BufferError Kafka (tentative %d/%d) — poll() pour libérer le buffer",
                attempt + 1, max_retries
            )
            producer.poll(1)  # bloquant 1 seconde max

    # Si on arrive ici, toutes les tentatives ont échoué
    log.error(
        "Message abandonné après %d tentatives (BufferError persistant)",
        max_retries
    )


def _configure_worker_logger(log_queue: Queue) -> None:
    """
    Configure le logger du process worker pour envoyer vers la QueueHandler.
    Identique à configure_worker_logger() dans logger.py.
    """
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.handlers.clear()

    qh = logging.handlers.QueueHandler(log_queue)
    qh.setLevel(logging.DEBUG)
    root.addHandler(qh)
    root.propagate = False
