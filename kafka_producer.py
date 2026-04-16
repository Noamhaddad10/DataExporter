"""
kafka_producer.py
=================
High-speed UDP receiver -> Kafka Producer.

This process replaces receiving_messages() from logger.py.
It does NO parsing: it sends raw bytes + packet_time into Kafka.
Parsing is delegated to the consumers (kafka_consumer.py).

Kafka message format:
    struct.pack("!d", packet_time)  [8 bytes, big-endian]
    + received_data                 [N bytes, raw UDP bytes]

Partitioning: pdu_type % NUM_PARTITIONS
    -> all EntityStatePdu go to the same partition
    -> ensures consistency of entity_locs_cache in each consumer
"""

import datetime
import logging
import logging.handlers
import os
import socket
import struct
import time
from multiprocessing import Queue, Event

from confluent_kafka import Producer, KafkaException

import kafka_config as cfg

# ---------------------------------------------------------------------------
# Logger (writes to the parent process log_queue)
# ---------------------------------------------------------------------------
log = logging.getLogger("kafka_producer")


# ---------------------------------------------------------------------------
# Delivery callback (called by producer.poll() in the main loop)
# ---------------------------------------------------------------------------
def _delivery_report(err, msg) -> None:
    """
    Callback invoked by confluent_kafka for each message after a delivery attempt.
    On error, we log but do NOT block UDP reception.
    Transient errors (broker unavailable) are handled upstream by retries.
    """
    if err is not None:
        log.error(
            "Kafka delivery failure | topic=%s partition=%d offset=%s | err=%s",
            msg.topic(), msg.partition(), msg.offset(), err
        )
    # On success, we don't log (too verbose at 20k msg/sec)


# ---------------------------------------------------------------------------
# Main producer process function
# ---------------------------------------------------------------------------
def run_producer(
    stop_event: Event,
    log_queue: Queue,
    start_time: float,
) -> None:
    """
    Entry point of the UDP receiver + Kafka producer process.

    Parameters
    ----------
    stop_event : multiprocessing.Event
        Set by kafka_main.py to trigger graceful shutdown.
    log_queue : multiprocessing.Queue
        Centralized logging queue (QueueHandler -> QueueListener in main).
    start_time : float
        Unix timestamp of pipeline start (used to compute relative packet_time).
    """
    # --- Initialize worker logger ---
    _configure_worker_logger(log_queue)
    log.info("Producer started -- PID %d", os.getpid())
    print(f"kafka_producer PID={os.getpid()}")

    # --- Initialize Kafka producer ---
    producer = _create_producer()

    # --- Initialize UDP socket ---
    sock = _create_udp_socket(cfg.PORT, cfg.MESSAGE_LENGTH)

    # --- Counters for stats ---
    count_sent: int       = 0   # messages successfully produced to Kafka
    count_errors: int     = 0   # messages where produce() raised an exception
    count_buffer_err: int = 0   # BufferError (internal Kafka queue full)
    last_stat_time: float = time.monotonic()

    log.info(
        "Listening UDP on port %d | topic=%s | %d partitions",
        cfg.PORT, cfg.KAFKA_TOPIC, cfg.KAFKA_NUM_PARTITIONS
    )

    try:
        while not stop_event.is_set():
            try:
                # ----------------------------------------------------------------
                # 1. UDP receive
                #    recvfrom() blocks until a datagram arrives.
                #    MESSAGE_LENGTH is the max buffer (262144 bytes by default).
                # ----------------------------------------------------------------
                received_data, _addr = sock.recvfrom(cfg.MESSAGE_LENGTH)

                # ----------------------------------------------------------------
                # 2. Compute relative packet_time
                #    (seconds elapsed since pipeline start)
                # ----------------------------------------------------------------
                packet_time: float = datetime.datetime.now().timestamp() - start_time

                # ----------------------------------------------------------------
                # 3. Build Kafka message
                #    8-byte header (packet_time big-endian) + raw PDU
                # ----------------------------------------------------------------
                kafka_value: bytes = struct.pack("!d", packet_time) + received_data

                # ----------------------------------------------------------------
                # 4. Determine partition
                #    The 3rd byte of the DIS PDU = pduType.
                #    Group same types together -> consistency of caches
                #    in consumers (notably entity_locs_cache).
                # ----------------------------------------------------------------
                pdu_type: int = received_data[2]
                partition: int = pdu_type % cfg.KAFKA_NUM_PARTITIONS

                # ----------------------------------------------------------------
                # 5. Send to Kafka (non-blocking)
                #    produce() adds to the producer's internal buffer.
                #    Actual network send happens via poll() or flush().
                # ----------------------------------------------------------------
                _produce_with_retry(producer, kafka_value, partition)
                count_sent += 1

                # ----------------------------------------------------------------
                # 6. Process callbacks (deliveries, errors)
                #    poll(0) = non-blocking: processes all pending callbacks.
                # ----------------------------------------------------------------
                producer.poll(0)

            except IndexError:
                # PDU too short to have a byte[2] -- ignore
                log.warning("Too-short PDU received (%d bytes) -- ignored", len(received_data))
                continue
            except OSError as exc:
                # Socket error (e.g. port already in use, interface down)
                log.error("UDP socket error: %s", exc, exc_info=True)
                count_errors += 1
                continue

            # ----------------------------------------------------------------
            # 7. Stats every 10 seconds
            # ----------------------------------------------------------------
            now = time.monotonic()
            if now - last_stat_time >= 10.0:
                elapsed = now - last_stat_time
                rate = count_sent / elapsed if elapsed > 0 else 0
                log.info(
                    "Producer stats | sent=%d | rate=%.0f msg/s | "
                    "errors=%d | buffer_errors=%d",
                    count_sent, rate, count_errors, count_buffer_err
                )
                print(
                    f"[producer] sent={count_sent} | "
                    f"rate={rate:.0f} msg/s | "
                    f"errors={count_errors}"
                )
                # Reset counters for the next stats window
                count_sent = 0
                count_errors = 0
                count_buffer_err = 0
                last_stat_time = now

    except KeyboardInterrupt:
        log.info("Producer interrupted by KeyboardInterrupt")

    finally:
        # ----------------------------------------------------------------
        # Graceful shutdown:
        # flush() waits for all buffered messages to be delivered
        # (or until the timeout is reached).
        # ----------------------------------------------------------------
        log.info("Flushing Kafka producer...")
        remaining = producer.flush(timeout=30)
        if remaining > 0:
            log.warning(
                "%d messages could not be delivered before flush timeout", remaining
            )
        sock.close()
        log.info(
            "Producer stopped gracefully | total sent=%d | errors=%d",
            count_sent, count_errors
        )
        print(f"[producer] stopped -- {count_sent} msg sent, {count_errors} errors")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _create_producer() -> Producer:
    """
    Creates and returns a configured confluent_kafka Producer.
    Raises KafkaException if the initial broker connection fails.
    """
    try:
        producer = Producer(cfg.KAFKA_PRODUCER_CONFIG)
        log.info("Kafka Producer initialized | bootstrap=%s", cfg.KAFKA_BOOTSTRAP)
        return producer
    except KafkaException as exc:
        log.critical("Unable to create Kafka Producer: %s", exc)
        raise


def _create_udp_socket(port: int, message_length: int) -> socket.socket:
    """
    Creates a UDP socket with a 128 MB receive buffer.
    Same configuration as receiving_messages() in the original logger.py.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # OS receive buffer of 128 MB to absorb bursts without kernel-level loss
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 128 * 1024 * 1024)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # Non-blocking timeout to check stop_event periodically
    sock.settimeout(1.0)

    sock.bind(("", port))
    log.info("UDP socket bound on port %d | buffer=128MB", port)
    return sock


def _produce_with_retry(
    producer: Producer,
    value: bytes,
    partition: int,
    max_retries: int = 3,
) -> None:
    """
    Attempts to send a message to Kafka with retry on BufferError.

    BufferError occurs when the producer's internal buffer is saturated
    (queue.buffering.max.messages reached). We call poll() to free up
    space before retrying.

    Parameters
    ----------
    producer    : Producer confluent_kafka
    value       : bytes -- message payload (packet_time + pdu_raw)
    partition   : int   -- target partition
    max_retries : int   -- number of attempts before giving up
    """
    for attempt in range(max_retries):
        try:
            producer.produce(
                topic=cfg.KAFKA_TOPIC,
                value=value,
                partition=partition,
                on_delivery=_delivery_report,
            )
            return  # success

        except BufferError:
            # Internal buffer saturated -> process pending callbacks
            # to free space, then retry
            log.debug(
                "Kafka BufferError (attempt %d/%d) -- poll() to free buffer",
                attempt + 1, max_retries
            )
            producer.poll(1)  # blocking up to 1 second

    # If we reach here, all attempts failed
    log.error(
        "Message dropped after %d attempts (persistent BufferError)",
        max_retries
    )


def _configure_worker_logger(log_queue: Queue) -> None:
    """
    Configures the worker process logger to send to QueueHandler.
    Identical to configure_worker_logger() in logger.py.
    """
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.handlers.clear()

    qh = logging.handlers.QueueHandler(log_queue)
    qh.setLevel(logging.DEBUG)
    root.addHandler(qh)
    root.propagate = False
