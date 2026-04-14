"""
kafka_config.py
===============
Configuration Kafka centralisée pour le pipeline DIS → Kafka → SQL.

Ce module charge DataExporterConfig.json et expose toutes les constantes
nécessaires au producer, aux consumers et au main orchestrateur.

Règle : tout le reste du code importe depuis ici. On ne duplique jamais
une constante de config dans un autre fichier.
"""

import json
import os
import logging
from typing import Any

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Chargement de DataExporterConfig.json
# ---------------------------------------------------------------------------
_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "DataExporterConfig.json")

def _load_config() -> dict:
    """Charge et retourne le contenu de DataExporterConfig.json."""
    try:
        with open(_CONFIG_PATH, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Fichier de configuration introuvable : {_CONFIG_PATH}\n"
            "Vérifiez que DataExporterConfig.json est dans le répertoire du projet."
        )
    except json.JSONDecodeError as exc:
        raise ValueError(f"DataExporterConfig.json invalide : {exc}") from exc

# Chargement une seule fois à l'import du module
_RAW: dict = _load_config()

# ---------------------------------------------------------------------------
# Paramètres SQL / DIS hérités de la config existante
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

# Types de PDU acceptés (3ème byte du paquet DIS = pduType)
PDU_TYPE_LIST: list[int] = [1, 2, 3, 21, 33]

# ---------------------------------------------------------------------------
# Paramètres Kafka (section "kafka" de DataExporterConfig.json)
# ---------------------------------------------------------------------------
_KAFKA_CFG: dict = _RAW.get("kafka", {})

# Broker bootstrap
KAFKA_BOOTSTRAP: str    = _KAFKA_CFG.get("bootstrap_servers", "localhost:9092")

# Topic unique pour tous les PDU bruts
KAFKA_TOPIC: str        = _KAFKA_CFG.get("topic", "dis.raw")

# Nombre de partitions du topic (= nombre de consumers en parallèle)
KAFKA_NUM_PARTITIONS: int = _KAFKA_CFG.get("num_partitions", 4)

# Consumer group ID (utilisé par tous les consumers du pipeline)
KAFKA_GROUP_ID: str     = _KAFKA_CFG.get("group_id", "dis-export-group")

# Nombre de consumers à instancier dans kafka_main.py
KAFKA_NUM_CONSUMERS: int = _KAFKA_CFG.get("num_consumers", 4)

# ---------------------------------------------------------------------------
# Configuration Producer confluent_kafka
# ---------------------------------------------------------------------------
_PROD_CFG: dict = _KAFKA_CFG.get("producer", {})

KAFKA_PRODUCER_CONFIG: dict = {
    # Adresse du broker
    "bootstrap.servers": KAFKA_BOOTSTRAP,

    # Attendre X ms avant d'envoyer un batch → réduit les appels réseau
    # à 5ms on regroupe les PDU arrivant dans la même fenêtre
    "linger.ms": _PROD_CFG.get("linger_ms", 5),

    # Taille maximale d'un batch avant envoi forcé (64 KB)
    "batch.size": _PROD_CFG.get("batch_size", 65536),

    # Compression lz4 : meilleur ratio vitesse/taux pour les petits messages (~100-200 bytes)
    "compression.type": _PROD_CFG.get("compression_type", "lz4"),

    # acks=all : le broker confirme l'écriture sur toutes les répliques → zéro perte
    "acks": _PROD_CFG.get("acks", "all"),

    # Buffer interne du producer (nombre de messages en attente d'envoi)
    "queue.buffering.max.messages": 500_000,

    # Buffer interne en kilo-octets (1 GB)
    "queue.buffering.max.kbytes": 1_048_576,

    # Timeout de livraison : si le message n'est pas ACKé en 30s → erreur
    "delivery.timeout.ms": 30_000,

    # Retry automatique en cas d'erreur transitoire
    "retries": 5,
}

# ---------------------------------------------------------------------------
# Configuration Consumer confluent_kafka
# ---------------------------------------------------------------------------
_CONS_CFG: dict = _KAFKA_CFG.get("consumer", {})

KAFKA_CONSUMER_CONFIG: dict = {
    # Adresse du broker
    "bootstrap.servers": KAFKA_BOOTSTRAP,

    # Groupe de consommateurs : Kafka répartit les partitions entre les membres du groupe
    "group.id": KAFKA_GROUP_ID,

    # Si un consumer rejoint pour la première fois (pas d'offset sauvegardé),
    # partir du début du topic pour ne manquer aucun message
    "auto.offset.reset": _CONS_CFG.get("auto_offset_reset", "earliest"),

    # CRITIQUE : auto-commit désactivé → on commite manuellement APRÈS l'insert SQL
    # Garantie : si SQL échoue, l'offset n'est pas avancé → re-traitement au redémarrage
    "enable.auto.commit": False,

    # Attendre au minimum 1 KB de données avant de déclencher un fetch
    "fetch.min.bytes": _CONS_CFG.get("fetch_min_bytes", 1024),

    # Attendre au maximum 100ms si fetch.min.bytes n'est pas atteint
    "fetch.wait.max.ms": _CONS_CFG.get("fetch_wait_max_ms", 100),

    # Temps max entre deux poll() avant que Kafka considère le consumer mort
    # (300 secondes = 5 minutes, pour absorber les longs inserts SQL batch)
    "max.poll.interval.ms": _CONS_CFG.get("max_poll_interval_ms", 300_000),

    # Timeout heartbeat avec le broker
    "session.timeout.ms": _CONS_CFG.get("session_timeout_ms", 30_000),
}

# ---------------------------------------------------------------------------
# Format du message Kafka (documentation inline)
# ---------------------------------------------------------------------------
# Structure d'un message Kafka (value) :
#
#   ┌─────────────────────────┬──────────────────────────┐
#   │  packet_time  (8 bytes) │  pdu_raw_data  (N bytes) │
#   │  struct.pack("!d", t)   │  bytes bruts UDP reçus   │
#   └─────────────────────────┴──────────────────────────┘
#
# Reconversion pour LoggerPDU (format attendu par LoggerPduProcessor) :
#   logger_line = pdu_raw_data + b"line_divider" + struct.pack("d", packet_time)
#                                                  ↑ native endian ("d"), PAS "!d"
#
KAFKA_MSG_HEADER_SIZE: int = 8   # bytes réservés au packet_time dans chaque message
KAFKA_LINE_DIVIDER: bytes  = b"line_divider"


def get_raw_config() -> dict:
    """Retourne le dictionnaire brut chargé depuis DataExporterConfig.json."""
    return _RAW


def reload() -> None:
    """Recharge la configuration depuis le disque (utile en test)."""
    global _RAW
    _RAW = _load_config()
    log.info("Configuration rechargée depuis %s", _CONFIG_PATH)
