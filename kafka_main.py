"""
kafka_main.py
=============
Main entry point of the DIS -> Kafka -> SQL pipeline.

Replaces logger.py as the primary orchestrator in Kafka mode.
logger.py remains intact as a fallback (SharedMemory Ring Buffer mode).

Pipeline:
    1. Prerequisites check (Kafka broker, SQL Server)
    2. Logger file name resolution (same logic as logger.py)
    3. Startup row insertion into dbo.Loggers
    4. Launch of the UDP->Kafka producer (1 Process)
    5. Launch of the Kafka->SQL consumers (N Processes)
    6. Main loop with stats every 5 seconds
    7. Graceful shutdown on Ctrl+C

Graceful shutdown:
    - stop_event set -> all processes observe it
    - Consumers finish the current message and commit the offset
    - Producer flush() all pending messages
    - Wait for all processes to finish
"""

import datetime
import json
import logging
import logging.handlers
import multiprocessing
import os
import re
import sys
import time
import urllib.parse

import pandas as pd
import sqlalchemy
from confluent_kafka.admin import AdminClient, KafkaException, NewTopic

import kafka_config as cfg
from kafka_producer import run_producer
from kafka_consumer import run_consumer
from LoggerSQLExporter import LoggerSQLExporter

# ---------------------------------------------------------------------------
# Centralized log format (identical to logger.py)
# ---------------------------------------------------------------------------
LOG_FORMAT = "%(asctime)s | %(processName)s(%(process)d) | %(levelname)s | %(name)s | %(message)s"


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

def start_log_listener(log_queue: multiprocessing.Queue, log_file: str) -> logging.handlers.QueueListener:
    """Starts the centralized log listener (in the main process)."""
    fmt = logging.Formatter(LOG_FORMAT)
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    listener = logging.handlers.QueueListener(log_queue, fh, respect_handler_level=True)
    listener.start()
    return listener


def configure_main_logger(log_queue: multiprocessing.Queue) -> None:
    """Configures the logger for the main process."""
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.handlers.clear()
    qh = logging.handlers.QueueHandler(log_queue)
    qh.setLevel(logging.DEBUG)
    root.addHandler(qh)
    root.propagate = False


# ---------------------------------------------------------------------------
# Kafka connection check
# ---------------------------------------------------------------------------

def check_kafka_connection(bootstrap: str, timeout: float = 10.0) -> bool:
    """
    Verifies that the Kafka broker is reachable by listing topics.

    Parameters
    ----------
    bootstrap : str   -- e.g. "localhost:9092"
    timeout   : float -- max wait in seconds

    Returns True if the broker responds, False otherwise.
    """
    try:
        admin = AdminClient({"bootstrap.servers": bootstrap})
        metadata = admin.list_topics(timeout=timeout)
        log = logging.getLogger("main")
        log.info(
            "Kafka reachable | broker=%s | topics=%d",
            bootstrap, len(metadata.topics)
        )
        print(f"[main] Kafka OK -- broker={bootstrap} | {len(metadata.topics)} topic(s)")
        return True
    except KafkaException as exc:
        print(f"[main] ERROR: Kafka unreachable at {bootstrap}: {exc}")
        print("[main] Make sure kafka_setup.bat has been run and the broker is running.")
        return False


# ---------------------------------------------------------------------------
# SQL Server connection check
# ---------------------------------------------------------------------------

def check_sql_connection(db_name: str) -> bool:
    """
    Verifies that SQL Server is reachable.
    Returns True if connection succeeds, False otherwise.
    """
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={cfg.SQL_SERVER};"
        f"DATABASE={db_name};"
        f"Trusted_Connection=yes;"
    )
    engine = None
    try:
        engine = sqlalchemy.create_engine(
            "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(conn_str),
            pool_pre_ping=True,
        )
        with engine.connect():
            pass
        print(f"[main] SQL Server OK -- database={db_name}")
        return True
    except Exception as exc:
        print(f"[main] ERROR: SQL Server unreachable (database={db_name}): {exc}")
        print("[main] Make sure SQL Server Express is running and the database exists.")
        return False
    finally:
        if engine is not None:
            engine.dispose()


# ---------------------------------------------------------------------------
# Logger file name resolution (same logic as logger.py)
# ---------------------------------------------------------------------------

def _sql_conn_context(database_name: str):
    """Returns a SQLAlchemy connection to SQL Server (Windows Auth)."""
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={cfg.SQL_SERVER};"
        f"DATABASE={database_name};"
        f"Trusted_Connection=yes;"
    )
    return sqlalchemy.create_engine(
        "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(conn_str),
        pool_pre_ping=True,
    )


def resolve_logger_file(db_name: str, logger_raw: str) -> str:
    """
    Resolves the logger file name by querying dbo.Loggers.
    Identical to check_loggerfile_name() in logger.py, but without interactive input():
    automatically accepts the suggestion (daemon behaviour).

    Returns the final file name (e.g. "exp_14_4_3.lzma").

    When cfg.DISABLE_LOGGER_FILE_RESOLUTION is True, this function is
    short-circuited: the caller-provided name is used as-is (just adds
    .lzma suffix if missing). Used when an external orchestrator like
    launcher.py wants to control naming and prompt the user explicitly.
    """
    log = logging.getLogger("main")

    if cfg.DISABLE_LOGGER_FILE_RESOLUTION:
        final = logger_raw if logger_raw.endswith(".lzma") else f"{logger_raw}.lzma"
        log.info(
            "resolve_logger_file: skipped (DISABLE_LOGGER_FILE_RESOLUTION=True). "
            "Using '%s' as-is.", final
        )
        return final

    old_logger = logger_raw
    logger = logger_raw.removesuffix(".lzma")

    # Normalize name according to type (exp_, check_, integration_)
    logger_types = ("exp", "check", "integration")
    for logger_type in logger_types:
        if logger.lower().startswith(logger_type):
            now = datetime.datetime.now()
            logger = f"{logger_type}_{now.day}_{now.month}"
            break

    engine = _sql_conn_context(db_name)
    try:
        with engine.connect() as conn:
            query = sqlalchemy.text(
                "SELECT TOP 1 LoggerFile FROM Loggers "
                "WHERE LoggerFile LIKE :pattern ORDER BY WorldTime DESC"
            )
            exists_df = pd.read_sql_query(query, conn, params={"pattern": f"{logger}%"})
    except Exception as exc:
        print(f"[main] Warning: unable to read Loggers ({exc}). Using '{logger}_1.lzma'")
        return f"{logger}_1.lzma"
    finally:
        engine.dispose()

    if len(exists_df) != 0:
        last = exists_df["LoggerFile"][0].removesuffix(".lzma")
        nums = re.findall(r"\d+", last)
        if not nums or not last[-1].isdigit():
            logger = last + "_2"
        else:
            num_i = last.rfind(nums[-1])
            logger = last[:num_i] + str(int(nums[-1]) + 1)

        if old_logger.removesuffix(".lzma") != logger:
            print(f"[main] Logger name auto-renamed: {old_logger} -> {logger}.lzma")
    else:
        if not logger.endswith("_1") and not (
            datetime.datetime.now().month == 1 and logger.endswith("_1_1")
        ):
            logger += "_1"

    final_name = logger + ".lzma"

    # Update DataExporterConfig.json (same as logger.py)
    try:
        raw_cfg = cfg.get_raw_config()
        raw_cfg["logger_file"] = final_name
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "DataExporterConfig.json")
        with open(config_path, "w", encoding="utf-8") as fh:
            json.dump(raw_cfg, fh, indent=4)
        print(f"[main] Logger name: {final_name}")
    except Exception as exc:
        print(f"[main] Warning: unable to update DataExporterConfig.json: {exc}")

    return final_name


# ---------------------------------------------------------------------------
# Startup row insertion into dbo.Loggers
# ---------------------------------------------------------------------------

def register_logger_in_db(
    logger_file: str,
    start_time: float,
    stop_writing_event: multiprocessing.Event,
) -> "LoggerSQLExporter":
    """
    Inserts the startup row into dbo.Loggers.
    Identical to what logger.py does with lse.export("Loggers", ...).
    """
    start_logger_data = {
        "LoggerFile": logger_file,
        "Scenario": None if cfg.SCENARIO == "" else cfg.SCENARIO,
        "WorldTime": datetime.datetime.fromtimestamp(start_time),
        "ExerciseId": cfg.EXERCISE_ID,
    }
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
    lse.export("Loggers", start_logger_data)
    print(f"[main] Loggers row inserted for '{logger_file}'")
    return lse


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """
    Orchestrates the full Kafka pipeline startup.
    """
    multiprocessing.freeze_support()

    # ----------------------------------------------------------------
    # 1. Centralized logging
    # ----------------------------------------------------------------
    log_queue: multiprocessing.Queue = multiprocessing.Queue(maxsize=50_000)
    listener = start_log_listener(log_queue, "dis-kafka.log")
    configure_main_logger(log_queue)
    log = logging.getLogger("main")

    log.info("kafka_main started -- PID %d", os.getpid())
    print(f"[main] kafka_main PID={os.getpid()}")

    # ----------------------------------------------------------------
    # 2. Prerequisites check
    # ----------------------------------------------------------------
    print("\n[main] === Checking prerequisites ===")

    if not check_kafka_connection(cfg.KAFKA_BOOTSTRAP):
        log.critical("Kafka unreachable -- aborting")
        sys.exit(1)

    if not check_sql_connection(cfg.DB_NAME):
        log.critical("SQL Server unreachable -- aborting")
        sys.exit(1)

    print("[main] Prerequisites OK\n")

    # ----------------------------------------------------------------
    # 2bis. Fresh-start mode (skip pre-existing messages)
    #
    # When KAFKA_RESET_TOPIC_ON_STARTUP is True we generate a unique
    # consumer group id for this run AND tell consumers to start at
    # the end of the log. Net effect: any messages produced before
    # this run are ignored, this scenario starts on a clean slate.
    #
    # We deliberately do NOT delete the topic physically. Deleting
    # segments triggers a chronic "log dirs have failed" bug on
    # Windows when the OS still holds file handles. The fresh-start
    # approach achieves the same observable behavior without ever
    # asking the broker to remove disk segments.
    # ----------------------------------------------------------------
    if cfg.KAFKA_RESET_TOPIC_ON_STARTUP:
        run_id = int(time.time())
        effective_group_id = f"{cfg.KAFKA_GROUP_ID}-run-{run_id}"
        consumer_offset_reset = "latest"
        print(f"[main] Fresh-start mode: group_id={effective_group_id} (skipping any pre-existing messages)")
        log.info("Fresh-start mode active -- group_id=%s, auto.offset.reset=latest", effective_group_id)
    else:
        effective_group_id = cfg.KAFKA_GROUP_ID
        consumer_offset_reset = None  # use default from kafka_config

    # ----------------------------------------------------------------
    # 3. Logger file name resolution
    # ----------------------------------------------------------------
    try:
        logger_file = resolve_logger_file(cfg.DB_NAME, cfg.LOGGER_FILE)
    except Exception as exc:
        log.critical("Unable to resolve logger name: %s", exc)
        print(f"[main] ERROR resolving logger: {exc}")
        sys.exit(1)

    log.info("Logger file: %s", logger_file)

    # ----------------------------------------------------------------
    # 4. Start timestamp
    # ----------------------------------------------------------------
    start_time: float = datetime.datetime.now().timestamp()

    # ----------------------------------------------------------------
    # 5. Insert into dbo.Loggers
    # ----------------------------------------------------------------
    stop_writing_event_main = multiprocessing.Event()
    main_lse = None
    try:
        main_lse = register_logger_in_db(logger_file, start_time, stop_writing_event_main)
    except Exception as exc:
        log.error("Error inserting into Loggers: %s", exc, exc_info=True)
        print(f"[main] Warning: unable to insert into Loggers: {exc}")
        # Non-fatal -- continue

    # ----------------------------------------------------------------
    # 6. Shared shutdown event for all processes
    # ----------------------------------------------------------------
    stop_event = multiprocessing.Event()

    # ----------------------------------------------------------------
    # 7. Start Producer (1 process)
    # ----------------------------------------------------------------
    producer_process = multiprocessing.Process(
        target=run_producer,
        args=(stop_event, log_queue, start_time),
        name="KafkaProducer",
        daemon=False,
    )

    # ----------------------------------------------------------------
    # 8. Start Consumers (N processes)
    # ----------------------------------------------------------------
    consumer_processes = []
    for i in range(cfg.KAFKA_NUM_CONSUMERS):
        p = multiprocessing.Process(
            target=run_consumer,
            args=(i, stop_event, log_queue, logger_file, start_time,
                  effective_group_id, consumer_offset_reset),
            name=f"KafkaConsumer-{i}",
            daemon=False,
        )
        consumer_processes.append(p)

    # ----------------------------------------------------------------
    # 9. Start all processes
    # ----------------------------------------------------------------
    print(f"\n[main] Starting: 1 producer + {cfg.KAFKA_NUM_CONSUMERS} consumers")
    print(f"[main] Topic: {cfg.KAFKA_TOPIC} | Bootstrap: {cfg.KAFKA_BOOTSTRAP}")
    print(f"[main] Logger file: {logger_file}")
    print("[main] Press Ctrl+C to stop gracefully\n")

    producer_process.start()
    log.info("Producer started -- PID %d", producer_process.pid)

    for p in consumer_processes:
        p.start()
        log.info("Consumer started -- PID %d | name=%s", p.pid, p.name)

    # ----------------------------------------------------------------
    # 10. Main loop -- stats every 5 seconds
    # ----------------------------------------------------------------
    try:
        while True:
            time.sleep(5.0)

            # Check that processes are still running
            alive_producer = producer_process.is_alive()
            alive_consumers = [p.is_alive() for p in consumer_processes]

            if not alive_producer:
                log.error("Producer process stopped unexpectedly -- restarting")
                print("[main] WARNING: Producer stopped -- restarting...")
                producer_process = multiprocessing.Process(
                    target=run_producer,
                    args=(stop_event, log_queue, start_time),
                    name="KafkaProducer",
                    daemon=False,
                )
                producer_process.start()

            for i, (p, alive) in enumerate(zip(consumer_processes, alive_consumers)):
                if not alive:
                    log.error("Consumer %d stopped unexpectedly -- restarting", i)
                    print(f"[main] WARNING: Consumer-{i} stopped -- restarting...")
                    new_p = multiprocessing.Process(
                        target=run_consumer,
                        args=(i, stop_event, log_queue, logger_file, start_time,
                              effective_group_id, consumer_offset_reset),
                        name=f"KafkaConsumer-{i}",
                        daemon=False,
                    )
                    consumer_processes[i] = new_p
                    new_p.start()

            # Print status
            n_alive = sum(alive_consumers)
            print(
                f"[main] status: producer={'OK' if alive_producer else 'KO'} | "
                f"consumers={n_alive}/{cfg.KAFKA_NUM_CONSUMERS} active | "
                f"logger={logger_file}"
            )

    except KeyboardInterrupt:
        print("\n[main] Ctrl+C received -- graceful shutdown in progress...")
        log.info("Shutdown triggered by KeyboardInterrupt")

    # ----------------------------------------------------------------
    # 11. Graceful shutdown
    # ----------------------------------------------------------------
    print("[main] Signaling stop to consumers and producer...")
    stop_event.set()

    # Wait for consumers to finish their in-progress commits
    print("[main] Waiting for consumers (max 30s)...")
    for p in consumer_processes:
        p.join(timeout=30)
        if p.is_alive():
            log.warning("Consumer %s did not stop within 30s -- terminate()", p.name)
            p.terminate()
            p.join(timeout=5)

    # Wait for producer to flush its messages
    print("[main] Waiting for producer (max 30s)...")
    producer_process.join(timeout=30)
    if producer_process.is_alive():
        log.warning("Producer did not stop within 30s -- terminate()")
        producer_process.terminate()
        producer_process.join(timeout=5)

    # Close main LoggerSQLExporter BEFORE stopping log listener,
    # so any warning emitted by close() still reaches dis-kafka.log
    if main_lse is not None:
        try:
            main_lse.close()
        except Exception as exc:
            log.warning("main_lse.close() failed: %s", exc)

    # Stop the log listener
    while not log_queue.empty():
        time.sleep(0.1)
    listener.stop()

    print("\n[main] === Pipeline stopped gracefully ===")
    print(f"[main] Logger file: {logger_file}")
    print("[main] Check dis-kafka.log for details")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    main()
