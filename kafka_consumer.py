"""
kafka_consumer.py
=================
Kafka Consumer -> Parse PDU -> SQL Export.

This process combines the logic of parsing_messages() + export_process_func()
from logger.py, removing all dependency on the SharedMemory Ring Buffer.

Each instance of this process:
  - consumes messages from one or more Kafka partitions
  - deserializes the message: packet_time (8 bytes big-endian) + pdu_raw_data
  - filters by pduType and exerciseID
  - reconstructs the LoggerPDU format expected by LoggerPduProcessor
  - calls lpp.process() -> fills its local processing_queue
  - drains the processing_queue -> SQL export via LoggerSQLExporter
  - commits the Kafka offset AFTER the SQL insert -> zero data loss guaranteed

IMPORTANT: each consumer has its own instances of:
    - LoggerPduProcessor (and its caches: entity_locs_cache, etc.)
    - LoggerSQLExporter  (and its Exporters / SQL threads)
    - processing_queue   (local Queue, not shared)
This avoids any contention between parallel consumers.
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
# Main consumer process function
# ---------------------------------------------------------------------------
def run_consumer(
    consumer_id: int,
    stop_event: Event,
    log_queue: Queue,
    logger_file: str,
    start_time: float,
) -> None:
    """
    Entry point of the Kafka consumer process.

    Parameters
    ----------
    consumer_id : int
        Unique consumer identifier (0..N-1), used in logs.
    stop_event : multiprocessing.Event
        Set by kafka_main.py to trigger graceful shutdown.
    log_queue : multiprocessing.Queue
        Centralized logging queue.
    logger_file : str
        Current logger file name (e.g. "exp_14_4_3.lzma").
        Injected by kafka_main.py after name resolution.
    start_time : float
        Unix timestamp of pipeline start.
    """
    # --- Initialize worker logger ---
    _configure_worker_logger(log_queue)
    log.info("Consumer %d started -- PID %d", consumer_id, os.getpid())
    print(f"kafka_consumer[{consumer_id}] PID={os.getpid()}")

    # --- Local queue (not shared) to route parsed PDUs to the exporter ---
    # LoggerPduProcessor writes to this queue via lpp.process()
    # We drain it immediately after each message
    processing_queue: Queue = Queue()

    # --- Initialize LoggerPduProcessor ---
    # entity_locations_per_second is distributed across consumers
    # (same logic as distribute_locs() in logger.py)
    locs_per_sec = max(1, cfg.ENTITY_LOCS_PER_SEC // cfg.KAFKA_NUM_CONSUMERS)
    lpp = LoggerPduProcessor(
        processing_queue,
        locs_per_sec,
        logger_file,
        start_time,
        process_entities=True,
        process_aggregates=True,
    )
    log.info("Consumer %d | LoggerPduProcessor initialized", consumer_id)

    # --- Initialize LoggerSQLExporter ---
    # Local stop_writing_event: the consumer manages it itself
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
    log.info("Consumer %d | LoggerSQLExporter initialized", consumer_id)

    # --- Initialize Kafka Consumer ---
    consumer = _create_consumer(consumer_id)

    # --- Counters for stats ---
    count_consumed: int   = 0   # messages received from Kafka
    count_exported: int   = 0   # messages exported to SQL
    count_skipped: int    = 0   # messages filtered (wrong pduType or exerciseID)
    count_errors: int     = 0   # parsing or export errors
    last_stat_time: float = time.monotonic()

    try:
        # Subscribe to topic -> Kafka rebalances partitions among group consumers
        consumer.subscribe([cfg.KAFKA_TOPIC])
        log.info("Consumer %d | Subscribed to topic '%s'", consumer_id, cfg.KAFKA_TOPIC)

        while not stop_event.is_set():
            # ----------------------------------------------------------------
            # 1. Poll Kafka -- timeout 1.0s to check stop_event periodically
            # ----------------------------------------------------------------
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # No message available within timeout -> loop back
                continue

            # ----------------------------------------------------------------
            # 2. Handle Kafka-level message errors
            # ----------------------------------------------------------------
            if msg.error():
                _handle_kafka_error(consumer_id, msg.error())
                continue

            # ----------------------------------------------------------------
            # 3. Deserialize Kafka message
            #
            #    Format: [packet_time 8 bytes big-endian] + [pdu_raw N bytes]
            #    The producer packed it with struct.pack("!d", packet_time).
            # ----------------------------------------------------------------
            try:
                value: bytes = msg.value()

                if len(value) < cfg.KAFKA_MSG_HEADER_SIZE:
                    log.warning(
                        "Consumer %d | Message too short (%d bytes) -- ignored",
                        consumer_id, len(value)
                    )
                    count_skipped += 1
                    _safe_commit(consumer, consumer_id)
                    continue

                # Read packet_time (big-endian, 8 bytes)
                packet_time: float = struct.unpack("!d", value[:cfg.KAFKA_MSG_HEADER_SIZE])[0]

                # Raw bytes of the DIS PDU
                pdu_data: bytes = value[cfg.KAFKA_MSG_HEADER_SIZE:]

            except struct.error as exc:
                log.error(
                    "Consumer %d | Deserialization error at offset=%d: %s",
                    consumer_id, msg.offset(), exc
                )
                count_errors += 1
                _safe_commit(consumer, consumer_id)
                continue

            # ----------------------------------------------------------------
            # 4. Quick filter on pduType (byte[2] of DIS PDU)
            #    Avoids creating a LoggerPDU for unsupported types.
            # ----------------------------------------------------------------
            try:
                pdu_type: int = pdu_data[2]
            except IndexError:
                log.warning("Consumer %d | PDU too short -- ignored", consumer_id)
                count_skipped += 1
                _safe_commit(consumer, consumer_id)
                continue

            if pdu_type not in cfg.PDU_TYPE_LIST:
                count_skipped += 1
                _safe_commit(consumer, consumer_id)
                continue

            # ----------------------------------------------------------------
            # 5. Build LoggerPDU via from_parts()
            #
            #    Fix BUG 2: bypasses b"line_divider" serialization entirely
            #    to eliminate any collision risk if PDU bytes accidentally
            #    contain that sequence.
            #    LoggerPDU.from_parts() creates the object directly from
            #    raw bytes and packet_time.
            # ----------------------------------------------------------------
            try:
                received_pdu = LoggerPDU.from_parts(pdu_data, packet_time)

            except Exception as exc:
                log.error(
                    "Consumer %d | Error creating LoggerPDU at offset=%d: %s",
                    consumer_id, msg.offset(), exc
                )
                count_errors += 1
                _safe_commit(consumer, consumer_id)
                continue

            # ----------------------------------------------------------------
            # 6. Filter on exerciseID
            #    Identical to the filter in parsing_messages() in logger.py.
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
            # 7. Process PDU via LoggerPduProcessor
            #    Fills processing_queue with (table_name, [dict_data])
            # ----------------------------------------------------------------
            try:
                lpp.process(received_pdu)
            except Exception as exc:
                log.error(
                    "Consumer %d | Error in lpp.process() at offset=%d: %s\n%s",
                    consumer_id, msg.offset(), exc, traceback.format_exc()
                )
                count_errors += 1
                _safe_commit(consumer, consumer_id)
                continue

            # ----------------------------------------------------------------
            # 8. Drain processing_queue -> SQL export
            #
            #    Export everything lpp put in the queue BEFORE committing
            #    the Kafka offset. Guarantee: if SQL fails, we don't commit
            #    -> message will be reprocessed on restart.
            # ----------------------------------------------------------------
            sql_ok: bool = _drain_and_export(
                processing_queue, lse, consumer_id, msg.offset()
            )
            count_consumed += 1

            if sql_ok:
                count_exported += 1
                # ----------------------------------------------------------------
                # 9. COMMIT Kafka OFFSET -- synchronous, AFTER successful SQL
                #    enable.auto.commit=False -> we control exactly when
                #    the offset advances.
                # ----------------------------------------------------------------
                _safe_commit(consumer, consumer_id)
            else:
                # SQL export failed -> do not commit the offset
                # The consumer will reprocess this message on next start
                count_errors += 1
                log.warning(
                    "Consumer %d | SQL export failed for offset=%d -- offset not committed",
                    consumer_id, msg.offset()
                )

            # ----------------------------------------------------------------
            # 10. Stats every 10 seconds
            # ----------------------------------------------------------------
            now = time.monotonic()
            if now - last_stat_time >= 10.0:
                elapsed = now - last_stat_time
                rate_in  = count_consumed / elapsed if elapsed > 0 else 0
                rate_out = count_exported / elapsed if elapsed > 0 else 0
                log.info(
                    "Consumer %d stats | consumed=%d (%.0f/s) | exported=%d (%.0f/s) | "
                    "skipped=%d | errors=%d",
                    consumer_id,
                    count_consumed, rate_in,
                    count_exported, rate_out,
                    count_skipped, count_errors,
                )
                print(
                    f"[consumer-{consumer_id}] "
                    f"consumed={count_consumed} ({rate_in:.0f}/s) | "
                    f"exported={count_exported} ({rate_out:.0f}/s) | "
                    f"errors={count_errors}"
                )
                count_consumed = 0
                count_exported = 0
                count_skipped  = 0
                count_errors   = 0
                last_stat_time = now

    except KafkaException as exc:
        log.critical("Consumer %d | Fatal KafkaException: %s", consumer_id, exc)
        print(f"[consumer-{consumer_id}] FATAL Kafka ERROR: {exc}")

    except KeyboardInterrupt:
        log.info("Consumer %d | Interrupted by KeyboardInterrupt", consumer_id)

    finally:
        # ----------------------------------------------------------------
        # Graceful shutdown:
        # 1. Signal LoggerSQLExporter to stop its timers
        # 2. Close Kafka consumer (final commit of pending offsets)
        # ----------------------------------------------------------------
        log.info("Consumer %d | Closing...", consumer_id)
        stop_writing_event.set()  # stop SQL timers in LoggerSQLExporter

        # Final drain of the queue before closing
        _drain_and_export(processing_queue, lse, consumer_id, offset=-1)

        consumer.close()
        log.info(
            "Consumer %d | Stopped gracefully | total consumed=%d | exported=%d | errors=%d",
            consumer_id, count_consumed, count_exported, count_errors
        )
        print(f"[consumer-{consumer_id}] stopped gracefully")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _create_consumer(consumer_id: int) -> Consumer:
    """
    Creates and returns a configured confluent_kafka Consumer.
    Each consumer has a unique client.id to facilitate monitoring.
    """
    config = dict(cfg.KAFKA_CONSUMER_CONFIG)
    config["client.id"] = f"dis-consumer-{consumer_id}"

    try:
        consumer = Consumer(config)
        log.info("Consumer %d | Kafka Consumer initialized | bootstrap=%s", consumer_id, cfg.KAFKA_BOOTSTRAP)
        return consumer
    except KafkaException as exc:
        log.critical("Unable to create Consumer %d: %s", consumer_id, exc)
        raise


def _handle_kafka_error(consumer_id: int, error: KafkaError) -> None:
    """
    Handles errors returned by consumer.poll().

    - PARTITION_EOF: normal information, not an error -> continue silently
    - Other errors: log as WARNING (transient errors self-recover)
    """
    if error.code() == KafkaError._PARTITION_EOF:
        # End of partition reached -- normal in "earliest" mode when all messages are read
        # Do not log to avoid spam
        pass
    else:
        log.warning(
            "Consumer %d | Kafka error: code=%s msg=%s fatal=%s",
            consumer_id, error.code(), error.str(), error.fatal()
        )


def _drain_and_export(
    processing_queue: Queue,
    lse: LoggerSQLExporter,
    consumer_id: int,
    offset: int,
) -> bool:
    """
    Drains the processing_queue and inserts each (table, data) into SQL SYNCHRONOUSLY.

    Fix BUG 3: uses lse.insert_sync() instead of lse.export() (async).
    This guarantees that the SQL insert is committed to the database BEFORE the caller
    commits the Kafka offset. On crash between insert and Kafka commit: the message
    will be reprocessed (at-least-once), but NO data will be silently lost.

    Returns True if all inserts succeeded, False otherwise.
    On SQL failure, we keep draining to avoid blocking the queue,
    but return False so the caller does NOT commit the offset.

    Parameters
    ----------
    processing_queue : Queue -- filled by lpp.process()
    lse              : LoggerSQLExporter -- current consumer instance
    consumer_id      : int   -- for logging
    offset           : int   -- current Kafka offset (for logging)
    """
    all_ok = True

    while not processing_queue.empty():
        try:
            table, data = processing_queue.get_nowait()
            ok = lse.insert_sync(table, data)
            if not ok:
                log.error(
                    "Consumer %d | insert_sync failed table=%s offset=%d",
                    consumer_id, table, offset
                )
                all_ok = False
        except Exception as exc:
            log.error(
                "Consumer %d | SQL export error at offset=%d: %s\n%s",
                consumer_id, offset, exc, traceback.format_exc()
            )
            all_ok = False

    return all_ok


def _safe_commit(consumer: Consumer, consumer_id: int) -> None:
    """
    Performs a synchronous offset commit.
    Synchronous (asynchronous=False) to guarantee the offset is persisted
    before continuing.
    Absorbs exceptions to avoid killing the consumer on a commit failure.
    """
    try:
        consumer.commit(asynchronous=False)
    except KafkaException as exc:
        log.warning("Consumer %d | Offset commit failed: %s", consumer_id, exc)


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
