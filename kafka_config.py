"""
kafka_config.py
===============
Centralized Kafka configuration for the DIS -> Kafka -> SQL pipeline.

This module loads DataExporterConfig.json and exposes all constants
needed by the producer, consumers, and main orchestrator.

Rule: all other code imports from here. Constants are never duplicated
in any other file.
"""

import json
import os
import logging
from typing import Any

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Load DataExporterConfig.json
# ---------------------------------------------------------------------------
_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "DataExporterConfig.json")

def _load_config() -> dict:
    """Loads and returns the contents of DataExporterConfig.json."""
    try:
        with open(_CONFIG_PATH, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Configuration file not found: {_CONFIG_PATH}\n"
            "Make sure DataExporterConfig.json is in the project directory."
        )
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid DataExporterConfig.json: {exc}") from exc

# Loaded once at module import
_RAW: dict = _load_config()

# ---------------------------------------------------------------------------
# SQL / DIS parameters inherited from existing config
# ---------------------------------------------------------------------------
EXERCISE_ID: int        = _RAW["exercise_id"]
DB_NAME: str            = _RAW["database_name"]
NEW_DB: bool            = _RAW["new_database"]
LOGGER_FILE: str        = _RAW["logger_file"]
PORT: int               = _RAW["PORT"]
MESSAGE_LENGTH: int     = _RAW["message_length"]
EXPORT_DELAY: float     = _RAW["export_delay"]
EXPORT_SIZE: int        = _RAW["export_size"]
ENTITY_LOCS_PER_SEC: int= _RAW["entity_locations_per_second"]
TRACKED_TABLES: list    = _RAW["tracked_tables"].replace(" ", "").split(",")
SCENARIO: Any           = _RAW.get("Scenario", None)
SQL_SERVER: str         = _RAW.get("sql_server", r"localhost\SQLEXPRESS")

# Accepted PDU types (3rd byte of DIS packet = pduType)
PDU_TYPE_LIST: list[int] = [1, 2, 3, 21, 33]

# ---------------------------------------------------------------------------
# Kafka parameters (section "kafka" of DataExporterConfig.json)
# ---------------------------------------------------------------------------
_KAFKA_CFG: dict = _RAW.get("kafka", {})

# Broker bootstrap
KAFKA_BOOTSTRAP: str    = _KAFKA_CFG.get("bootstrap_servers", "localhost:9092")

# Single topic for all raw PDUs
KAFKA_TOPIC: str        = _KAFKA_CFG.get("topic", "dis.raw")

# Number of topic partitions (= number of parallel consumers)
KAFKA_NUM_PARTITIONS: int = _KAFKA_CFG.get("num_partitions", 4)

# Consumer group ID (shared by all pipeline consumers)
KAFKA_GROUP_ID: str     = _KAFKA_CFG.get("group_id", "dis-export-group")

# Number of consumers to instantiate in kafka_main.py
KAFKA_NUM_CONSUMERS: int = _KAFKA_CFG.get("num_consumers", 4)

# ---------------------------------------------------------------------------
# confluent_kafka Producer configuration
# ---------------------------------------------------------------------------
_PROD_CFG: dict = _KAFKA_CFG.get("producer", {})

KAFKA_PRODUCER_CONFIG: dict = {
    # Broker address
    "bootstrap.servers": KAFKA_BOOTSTRAP,

    # Wait X ms before sending a batch -> reduces network calls
    # at 5ms we group PDUs arriving in the same window
    "linger.ms": _PROD_CFG.get("linger_ms", 5),

    # Maximum batch size before forced send (64 KB)
    "batch.size": _PROD_CFG.get("batch_size", 65536),

    # lz4 compression: best speed/ratio for small messages (~100-200 bytes)
    "compression.type": _PROD_CFG.get("compression_type", "lz4"),

    # acks=all: broker confirms write on all replicas -> zero data loss
    "acks": _PROD_CFG.get("acks", "all"),

    # Internal producer buffer (number of messages awaiting delivery)
    "queue.buffering.max.messages": 500_000,

    # Internal buffer in kilobytes (1 GB)
    "queue.buffering.max.kbytes": 1_048_576,

    # Delivery timeout: if message is not ACKed within 30s -> error
    "delivery.timeout.ms": 30_000,

    # Automatic retry on transient errors
    "retries": 5,
}

# ---------------------------------------------------------------------------
# confluent_kafka Consumer configuration
# ---------------------------------------------------------------------------
_CONS_CFG: dict = _KAFKA_CFG.get("consumer", {})

KAFKA_CONSUMER_CONFIG: dict = {
    # Broker address
    "bootstrap.servers": KAFKA_BOOTSTRAP,

    # Consumer group: Kafka distributes partitions among group members
    "group.id": KAFKA_GROUP_ID,

    # If a consumer joins for the first time (no saved offset),
    # start from the beginning of the topic to not miss any message
    "auto.offset.reset": _CONS_CFG.get("auto_offset_reset", "earliest"),

    # CRITICAL: auto-commit disabled -> we commit manually AFTER the SQL insert
    # Guarantee: if SQL fails, offset is not advanced -> reprocessed on restart
    "enable.auto.commit": False,

    # Minimum data to fetch before triggering a fetch request (1 KB)
    "fetch.min.bytes": _CONS_CFG.get("fetch_min_bytes", 1024),

    # Maximum wait if fetch.min.bytes is not reached (100ms)
    "fetch.wait.max.ms": _CONS_CFG.get("fetch_wait_max_ms", 100),

    # Max time between two poll() calls before Kafka considers the consumer dead
    # (300 seconds = 5 minutes, to absorb long SQL batch inserts)
    "max.poll.interval.ms": _CONS_CFG.get("max_poll_interval_ms", 300_000),

    # Heartbeat timeout with the broker
    "session.timeout.ms": _CONS_CFG.get("session_timeout_ms", 30_000),
}

# ---------------------------------------------------------------------------
# Kafka message format (inline documentation)
# ---------------------------------------------------------------------------
# Structure of a Kafka message (value):
#
#   ┌─────────────────────────┬──────────────────────────┐
#   │  packet_time  (8 bytes) │  pdu_raw_data  (N bytes) │
#   │  struct.pack("!d", t)   │  raw UDP bytes received  │
#   └─────────────────────────┴──────────────────────────┘
#
# Conversion back for LoggerPDU (format expected by LoggerPduProcessor):
#   logger_line = pdu_raw_data + b"line_divider" + struct.pack("d", packet_time)
#                                                  ↑ native endian ("d"), NOT "!d"
#
KAFKA_MSG_HEADER_SIZE: int = 8   # bytes reserved for packet_time in each message
KAFKA_LINE_DIVIDER: bytes  = b"line_divider"


def get_raw_config() -> dict:
    """Returns the raw dictionary loaded from DataExporterConfig.json."""
    return _RAW


def reload() -> None:
    """Reloads configuration from disk (useful in tests)."""
    global _RAW
    _RAW = _load_config()
    log.info("Configuration reloaded from %s", _CONFIG_PATH)
