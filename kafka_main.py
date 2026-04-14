"""
kafka_main.py
=============
Nouveau point d'entrée du pipeline DIS → Kafka → SQL.

Remplace logger.py comme orchestrateur principal en mode Kafka.
logger.py reste intact comme fallback (mode SharedMemory Ring Buffer).

Pipeline :
    1. Vérification des prérequis (Kafka broker, SQL Server)
    2. Résolution du nom du fichier logger (même logique que logger.py)
    3. Insertion de la ligne de démarrage dans dbo.Loggers
    4. Lancement du producer UDP→Kafka (1 Process)
    5. Lancement des consumers Kafka→SQL (N Process)
    6. Boucle principale avec stats toutes les 5 secondes
    7. Graceful shutdown sur Ctrl+C

Graceful shutdown :
    - stop_event signalé → tous les processes l'observent
    - Les consumers finissent le message en cours et commitent l'offset
    - Le producer flush() tous les messages en attente
    - On attend la fin de tous les processes
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
import traceback
import urllib.parse

import pandas as pd
import pyodbc
import sqlalchemy
from confluent_kafka.admin import AdminClient, KafkaException

import kafka_config as cfg
from kafka_producer import run_producer
from kafka_consumer import run_consumer
from LoggerSQLExporter import LoggerSQLExporter

# ---------------------------------------------------------------------------
# Format de log centralisé (identique à logger.py)
# ---------------------------------------------------------------------------
LOG_FORMAT = "%(asctime)s | %(processName)s(%(process)d) | %(levelname)s | %(name)s | %(message)s"


# ---------------------------------------------------------------------------
# Helpers logging
# ---------------------------------------------------------------------------

def start_log_listener(log_queue: multiprocessing.Queue, log_file: str) -> logging.handlers.QueueListener:
    """Démarre le listener de log centralisé (dans le process principal)."""
    fmt = logging.Formatter(LOG_FORMAT)
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    listener = logging.handlers.QueueListener(log_queue, fh, respect_handler_level=True)
    listener.start()
    return listener


def configure_main_logger(log_queue: multiprocessing.Queue) -> None:
    """Configure le logger du process principal (main)."""
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.handlers.clear()
    qh = logging.handlers.QueueHandler(log_queue)
    qh.setLevel(logging.DEBUG)
    root.addHandler(qh)
    root.propagate = False


# ---------------------------------------------------------------------------
# Vérification de la connexion Kafka
# ---------------------------------------------------------------------------

def check_kafka_connection(bootstrap: str, timeout: float = 10.0) -> bool:
    """
    Vérifie que le broker Kafka est accessible en listant les topics.

    Paramètres
    ----------
    bootstrap : str   — ex: "localhost:9092"
    timeout   : float — secondes d'attente max

    Retourne True si le broker répond, False sinon.
    """
    try:
        admin = AdminClient({"bootstrap.servers": bootstrap})
        metadata = admin.list_topics(timeout=timeout)
        log = logging.getLogger("main")
        log.info(
            "Kafka accessible | broker=%s | topics=%d",
            bootstrap, len(metadata.topics)
        )
        print(f"[main] Kafka OK — broker={bootstrap} | {len(metadata.topics)} topic(s)")
        return True
    except KafkaException as exc:
        print(f"[main] ERREUR : Kafka inaccessible sur {bootstrap} : {exc}")
        print("[main] Vérifiez que kafka_setup.bat a été exécuté et que le broker tourne.")
        return False


# ---------------------------------------------------------------------------
# Vérification de la connexion SQL Server
# ---------------------------------------------------------------------------

def check_sql_connection(db_name: str) -> bool:
    """
    Vérifie que SQL Server est accessible.
    Retourne True si la connexion réussit, False sinon.
    """
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER=localhost\\SQLEXPRESS;"
        f"DATABASE={db_name};"
        f"Trusted_Connection=yes;"
    )
    try:
        engine = sqlalchemy.create_engine(
            "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(conn_str),
            pool_pre_ping=True,
        )
        with engine.connect():
            pass
        print(f"[main] SQL Server OK — base={db_name}")
        return True
    except Exception as exc:
        print(f"[main] ERREUR : SQL Server inaccessible (base={db_name}) : {exc}")
        print("[main] Vérifiez que SQL Server Express tourne et que la base existe.")
        return False


# ---------------------------------------------------------------------------
# Résolution du nom du fichier logger (même logique que logger.py)
# ---------------------------------------------------------------------------

def _sql_conn_context(database_name: str):
    """Retourne une connexion SQLAlchemy à SQL Server (Windows Auth)."""
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER=localhost\\SQLEXPRESS;"
        f"DATABASE={database_name};"
        f"Trusted_Connection=yes;"
    )
    return sqlalchemy.create_engine(
        "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(conn_str),
        pool_pre_ping=True,
    )


def resolve_logger_file(db_name: str, logger_raw: str) -> str:
    """
    Résout le nom du fichier logger en interrogeant dbo.Loggers.
    Identique à check_loggerfile_name() dans logger.py, mais sans input() interactif :
    accepte automatiquement la suggestion (comportement daemon).

    Retourne le nom de fichier final (ex: "exp_14_4_3.lzma").
    """
    old_logger = logger_raw
    logger = logger_raw.removesuffix(".lzma")

    # Normalise le nom selon le type (exp_, check_, integration_)
    logger_types = ("exp", "check", "integration")
    for logger_type in logger_types:
        if logger.lower().startswith(logger_type):
            now = datetime.datetime.now()
            logger = f"{logger_type}_{now.day}_{now.month}"
            break

    engine = _sql_conn_context(db_name)
    try:
        with engine.connect() as conn:
            query = (
                f"SELECT TOP 1 LoggerFile FROM Loggers "
                f"WHERE LoggerFile LIKE '{logger}%' ORDER BY WorldTime DESC"
            )
            exists_df = pd.read_sql_query(query, conn)
    except Exception as exc:
        print(f"[main] Avertissement : impossible de lire Loggers ({exc}). Utilisation de '{logger}_1.lzma'")
        return f"{logger}_1.lzma"

    if len(exists_df) != 0:
        last = exists_df["LoggerFile"][0].removesuffix(".lzma")
        nums = re.findall(r"\d+", last)
        if not nums or not last[-1].isdigit():
            logger = last + "_2"
        else:
            num_i = last.rfind(nums[-1])
            logger = last[:num_i] + str(int(nums[-1]) + 1)

        if old_logger.removesuffix(".lzma") != logger:
            print(f"[main] Nom logger auto-renommé : {old_logger} → {logger}.lzma")
    else:
        if not logger.endswith("_1") and not (
            datetime.datetime.now().month == 1 and logger.endswith("_1_1")
        ):
            logger += "_1"

    final_name = logger + ".lzma"

    # Mise à jour du DataExporterConfig.json (comme logger.py)
    try:
        raw_cfg = cfg.get_raw_config()
        raw_cfg["logger_file"] = final_name
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "DataExporterConfig.json")
        with open(config_path, "w", encoding="utf-8") as fh:
            json.dump(raw_cfg, fh, indent=4)
        print(f"[main] Logger name : {final_name}")
    except Exception as exc:
        print(f"[main] Avertissement : impossible de mettre à jour DataExporterConfig.json : {exc}")

    return final_name


# ---------------------------------------------------------------------------
# Insertion de la ligne de démarrage dans dbo.Loggers
# ---------------------------------------------------------------------------

def register_logger_in_db(
    logger_file: str,
    start_time: float,
    stop_writing_event: multiprocessing.Event,
) -> None:
    """
    Insère la ligne de démarrage dans dbo.Loggers.
    Identique à ce que fait logger.py avec lse.export("Loggers", ...).
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
    print(f"[main] Ligne Loggers insérée pour '{logger_file}'")


# ---------------------------------------------------------------------------
# Point d'entrée principal
# ---------------------------------------------------------------------------

def main() -> None:
    """
    Orchestre le démarrage complet du pipeline Kafka.
    """
    multiprocessing.freeze_support()

    # ----------------------------------------------------------------
    # 1. Logging centralisé
    # ----------------------------------------------------------------
    log_queue: multiprocessing.Queue = multiprocessing.Queue(maxsize=50_000)
    listener = start_log_listener(log_queue, "dis-kafka.log")
    configure_main_logger(log_queue)
    log = logging.getLogger("main")

    log.info("kafka_main démarré — PID %d", os.getpid())
    print(f"[main] kafka_main PID={os.getpid()}")

    # ----------------------------------------------------------------
    # 2. Vérification des prérequis
    # ----------------------------------------------------------------
    print("\n[main] === Vérification des prérequis ===")

    if not check_kafka_connection(cfg.KAFKA_BOOTSTRAP):
        log.critical("Kafka inaccessible — arrêt")
        sys.exit(1)

    if not check_sql_connection(cfg.DB_NAME):
        log.critical("SQL Server inaccessible — arrêt")
        sys.exit(1)

    print("[main] Prérequis OK\n")

    # ----------------------------------------------------------------
    # 3. Résolution du nom du fichier logger
    # ----------------------------------------------------------------
    try:
        logger_file = resolve_logger_file(cfg.DB_NAME, cfg.LOGGER_FILE)
    except Exception as exc:
        log.critical("Impossible de résoudre le nom du logger : %s", exc)
        print(f"[main] ERREUR résolution logger : {exc}")
        sys.exit(1)

    log.info("Fichier logger : %s", logger_file)

    # ----------------------------------------------------------------
    # 4. Timestamp de démarrage
    # ----------------------------------------------------------------
    start_time: float = datetime.datetime.now().timestamp()

    # ----------------------------------------------------------------
    # 5. Insertion dans dbo.Loggers
    # ----------------------------------------------------------------
    stop_writing_event_main = multiprocessing.Event()
    try:
        register_logger_in_db(logger_file, start_time, stop_writing_event_main)
    except Exception as exc:
        log.error("Erreur insertion Loggers : %s", exc)
        print(f"[main] Avertissement : impossible d'insérer dans Loggers : {exc}")
        # Non fatal — on continue

    # ----------------------------------------------------------------
    # 6. Event de shutdown partagé entre tous les processes
    # ----------------------------------------------------------------
    stop_event = multiprocessing.Event()

    # ----------------------------------------------------------------
    # 7. Lancement du Producer (1 process)
    # ----------------------------------------------------------------
    producer_process = multiprocessing.Process(
        target=run_producer,
        args=(stop_event, log_queue, start_time),
        name="KafkaProducer",
        daemon=False,
    )

    # ----------------------------------------------------------------
    # 8. Lancement des Consumers (N processes)
    # ----------------------------------------------------------------
    consumer_processes = []
    for i in range(cfg.KAFKA_NUM_CONSUMERS):
        p = multiprocessing.Process(
            target=run_consumer,
            args=(i, stop_event, log_queue, logger_file, start_time),
            name=f"KafkaConsumer-{i}",
            daemon=False,
        )
        consumer_processes.append(p)

    # ----------------------------------------------------------------
    # 9. Démarrage
    # ----------------------------------------------------------------
    print(f"\n[main] Démarrage : 1 producer + {cfg.KAFKA_NUM_CONSUMERS} consumers")
    print(f"[main] Topic : {cfg.KAFKA_TOPIC} | Bootstrap : {cfg.KAFKA_BOOTSTRAP}")
    print(f"[main] Logger file : {logger_file}")
    print("[main] Ctrl+C pour arrêter proprement\n")

    producer_process.start()
    log.info("Producer démarré — PID %d", producer_process.pid)

    for p in consumer_processes:
        p.start()
        log.info("Consumer démarré — PID %d | name=%s", p.pid, p.name)

    # ----------------------------------------------------------------
    # 10. Boucle principale — stats toutes les 5 secondes
    # ----------------------------------------------------------------
    try:
        while True:
            time.sleep(5.0)

            # Vérification que les processes tournent encore
            alive_producer = producer_process.is_alive()
            alive_consumers = [p.is_alive() for p in consumer_processes]

            if not alive_producer:
                log.error("Le process Producer s'est arrêté de manière inattendue — redémarrage")
                print("[main] AVERTISSEMENT : Producer arrêté — redémarrage...")
                producer_process = multiprocessing.Process(
                    target=run_producer,
                    args=(stop_event, log_queue, start_time),
                    name="KafkaProducer",
                    daemon=False,
                )
                producer_process.start()

            for i, (p, alive) in enumerate(zip(consumer_processes, alive_consumers)):
                if not alive:
                    log.error("Consumer %d s'est arrêté de manière inattendue — redémarrage", i)
                    print(f"[main] AVERTISSEMENT : Consumer-{i} arrêté — redémarrage...")
                    new_p = multiprocessing.Process(
                        target=run_consumer,
                        args=(i, stop_event, log_queue, logger_file, start_time),
                        name=f"KafkaConsumer-{i}",
                        daemon=False,
                    )
                    consumer_processes[i] = new_p
                    new_p.start()

            # Affichage du statut
            n_alive = sum(alive_consumers)
            print(
                f"[main] statut : producer={'OK' if alive_producer else 'KO'} | "
                f"consumers={n_alive}/{cfg.KAFKA_NUM_CONSUMERS} actifs | "
                f"logger={logger_file}"
            )

    except KeyboardInterrupt:
        print("\n[main] Ctrl+C reçu — shutdown propre en cours...")
        log.info("Shutdown déclenché par KeyboardInterrupt")

    # ----------------------------------------------------------------
    # 11. Graceful shutdown
    # ----------------------------------------------------------------
    print("[main] Signalement d'arrêt aux consumers et au producer...")
    stop_event.set()

    # Attendre que les consumers finissent leurs commits en cours
    print("[main] Attente des consumers (max 30s)...")
    for p in consumer_processes:
        p.join(timeout=30)
        if p.is_alive():
            log.warning("Consumer %s ne s'est pas arrêté dans les 30s — terminate()", p.name)
            p.terminate()
            p.join(timeout=5)

    # Attendre que le producer flush ses messages
    print("[main] Attente du producer (max 30s)...")
    producer_process.join(timeout=30)
    if producer_process.is_alive():
        log.warning("Producer ne s'est pas arrêté dans les 30s — terminate()")
        producer_process.terminate()
        producer_process.join(timeout=5)

    # Arrêt du listener de log
    while not log_queue.empty():
        time.sleep(0.1)
    listener.stop()

    print("\n[main] === Pipeline arrêté proprement ===")
    print(f"[main] Logger file : {logger_file}")
    print("[main] Vérifiez dis-kafka.log pour les détails")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    main()
