"""
test_kafka_pipeline.py
======================
Tests end-to-end du pipeline Kafka DIS.

Ce script couvre 5 scenarios de test :
  Test 1 -- Connectivite Kafka (broker accessible, topic existant)
  Test 2 -- Round-trip Python -> Kafka -> Python (producer/consumer directs)
  Test 3 -- Pipeline complet : UDP -> Kafka -> SQL (FirePdu)
  Test 4 -- Charge : N messages, zero perte, throughput
  Test 5 -- Resilience : perte d'un consumer, reprise sans perte

Usage :
    # Tous les tests (kafka_main.py doit tourner en parallele pour Test 3+)
    python test_kafka_pipeline.py

    # Un test precis
    python test_kafka_pipeline.py --test 3

    # Mode charge (Test 4)
    python test_kafka_pipeline.py --test 4 --count 50000
"""

import argparse
import datetime
import socket
import struct
import sys
import time
import urllib.parse
from typing import Optional

import sqlalchemy
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient

import kafka_config as cfg

# ---------------------------------------------------------------------------
# Utilitaires d'affichage
# ---------------------------------------------------------------------------
PASS  = "[PASS]"
FAIL  = "[FAIL]"
INFO  = "[INFO]"
WARN  = "[WARN]"

def _ok(msg: str) -> None:
    print(f"  {PASS} {msg}")

def _fail(msg: str) -> None:
    print(f"  {FAIL} {msg}")

def _info(msg: str) -> None:
    print(f"  {INFO} {msg}")

def _warn(msg: str) -> None:
    print(f"  {WARN} {msg}")

def _section(title: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


# ---------------------------------------------------------------------------
# Construction d'un FirePdu DIS 7 valide (98 bytes)
# Identique a send_fire_pdu.py mais self-contained pour ce test
# ---------------------------------------------------------------------------

EXERCISE_ID = cfg.EXERCISE_ID
PDU_LENGTH  = 98

def build_fire_pdu(event_number: int, timestamp: int) -> bytes:
    """Construit un FirePdu DIS 7 valide (98 bytes, big-endian)."""
    pdu_superclass = struct.pack("!BBBBIHH",
        7, EXERCISE_ID, 2, 2,   # version, exerciseID, pduType, family
        timestamp, PDU_LENGTH, 0
    )
    pdu_ext      = struct.pack("!BB", 0, 0)
    warfare      = struct.pack("!HHHHHH", 1, 3101, 1, 1, 3101, 2)
    munition_id  = struct.pack("!HHH", 1, 3101, 100)
    event_id     = struct.pack("!HHH", 1, 3101, event_number)
    fire_mission = struct.pack("!I", event_number)
    location     = struct.pack("!ddd", 4_429_530.0, 3_094_568.0, 3_320_580.0)
    descriptor   = struct.pack("!BBHBBBBHHHH", 2, 2, 105, 1, 1, 1, 0, 1000, 100, 1, 1)
    velocity     = struct.pack("!fff", 100.0, 0.0, -50.0)
    range_f      = struct.pack("!f", 5000.0)

    packet = (pdu_superclass + pdu_ext + warfare + munition_id
              + event_id + fire_mission + location + descriptor
              + velocity + range_f)

    assert len(packet) == PDU_LENGTH, f"Taille incorrecte : {len(packet)}"
    assert packet[2] == 2
    return packet


# ---------------------------------------------------------------------------
# Connexion SQL pour les verifications
# ---------------------------------------------------------------------------

def _get_sql_engine(db_name: str) -> sqlalchemy.engine.Engine:
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER=localhost\\SQLEXPRESS;"
        f"DATABASE={db_name};"
        f"Trusted_Connection=yes;"
    )
    return sqlalchemy.create_engine(
        "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(conn_str),
        pool_pre_ping=True,
    )


def _count_fire_pdu(engine: sqlalchemy.engine.Engine, since: datetime.datetime) -> int:
    """Compte les lignes dans dis.FirePdu inserees depuis `since`."""
    with engine.connect() as conn:
        result = conn.execute(
            sqlalchemy.text(
                "SELECT COUNT(*) FROM dis.FirePdu WHERE WorldTime >= :since"
            ),
            {"since": since}
        )
        return result.scalar()


# ---------------------------------------------------------------------------
# TEST 1 -- Connectivite Kafka
# ---------------------------------------------------------------------------

def test_1_kafka_connectivity() -> bool:
    """Verifie que le broker Kafka est accessible et que le topic dis.raw existe."""
    _section("TEST 1 -- Connectivite Kafka")
    passed = True

    # 1a. Broker accessible
    try:
        admin = AdminClient({"bootstrap.servers": cfg.KAFKA_BOOTSTRAP})
        metadata = admin.list_topics(timeout=10.0)
        _ok(f"Broker accessible : {cfg.KAFKA_BOOTSTRAP}")
    except KafkaException as exc:
        _fail(f"Broker inaccessible : {exc}")
        _info("Lancez kafka_setup.bat pour demarrer Kafka")
        return False

    # 1b. Topic dis.raw existe
    if cfg.KAFKA_TOPIC in metadata.topics:
        topic = metadata.topics[cfg.KAFKA_TOPIC]
        _ok(f"Topic '{cfg.KAFKA_TOPIC}' trouve -- {len(topic.partitions)} partitions")
    else:
        _fail(f"Topic '{cfg.KAFKA_TOPIC}' introuvable")
        _info("kafka_setup.bat cree le topic automatiquement")
        passed = False

    # 1c. Nombre de partitions
    if cfg.KAFKA_TOPIC in metadata.topics:
        n = len(metadata.topics[cfg.KAFKA_TOPIC].partitions)
        if n == cfg.KAFKA_NUM_PARTITIONS:
            _ok(f"Partitions : {n} (attendu {cfg.KAFKA_NUM_PARTITIONS})")
        else:
            _warn(f"Partitions : {n} (attendu {cfg.KAFKA_NUM_PARTITIONS}) -- desequilibre consumers/partitions")

    return passed


# ---------------------------------------------------------------------------
# TEST 2 -- Round-trip Python -> Kafka -> Python
# ---------------------------------------------------------------------------

def test_2_roundtrip() -> bool:
    """Envoie un message de test et verifie qu'il est recu par un consumer."""
    _section("TEST 2 -- Round-trip Python -> Kafka -> Python")

    from confluent_kafka import TopicPartition as TP

    unique = str(time.time()).encode()
    test_payload = b"DIS-KAFKA-TEST-" + unique
    partition = 0

    # 1. Produire
    try:
        producer = Producer(cfg.KAFKA_PRODUCER_CONFIG)
        producer.produce(cfg.KAFKA_TOPIC, value=test_payload, partition=partition)
        producer.flush(timeout=10)
        _ok("Message produit dans Kafka partition=0")
    except KafkaException as exc:
        _fail(f"Echec producer : {exc}")
        return False

    # 2. Lire le high-watermark (offset du dernier message)
    try:
        c_check = Consumer({"bootstrap.servers": cfg.KAFKA_BOOTSTRAP, "group.id": "test-wm"})
        lo, hi = c_check.get_watermark_offsets(TP(cfg.KAFKA_TOPIC, partition), timeout=5)
        c_check.close()
        target_offset = hi - 1
        _ok(f"Watermark partition={partition} : lo={lo} hi={hi} -> lecture offset={target_offset}")
    except Exception as exc:
        _fail(f"Impossible de lire les watermarks : {exc}")
        return False

    # 3. Consommer depuis l'offset exact du message produit
    consumer = Consumer({
        "bootstrap.servers": cfg.KAFKA_BOOTSTRAP,
        "group.id": f"dis-test-rt-{int(time.time())}",
        "enable.auto.commit": False,
    })
    consumer.assign([TP(cfg.KAFKA_TOPIC, partition, target_offset)])

    found = False
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            _fail(f"Erreur consumer : {msg.error()}")
            break
        if unique in msg.value():
            found = True
            break

    consumer.close()

    if found:
        _ok("Message recu par le consumer -- round-trip OK")
        return True
    else:
        _fail("Message non recu dans les 10 secondes")
        return False


# ---------------------------------------------------------------------------
# TEST 3 -- Pipeline complet UDP -> Kafka -> SQL
# ---------------------------------------------------------------------------

def test_3_pipeline(count: int = 20, wait_seconds: int = 15) -> bool:
    """
    Envoie N FirePdu en UDP et verifie qu'ils arrivent dans dis.FirePdu.
    kafka_main.py doit tourner en parallele.
    """
    _section(f"TEST 3 -- Pipeline complet (N={count} FirePdu)")
    _info("PREREQUIS : kafka_main.py doit tourner en parallele")

    # Connexion SQL
    try:
        engine = _get_sql_engine(cfg.DB_NAME)
        with engine.connect():
            pass
        _ok(f"SQL Server accessible -- base={cfg.DB_NAME}")
    except Exception as exc:
        _fail(f"SQL Server inaccessible : {exc}")
        return False

    # Timestamp de reference pour compter uniquement les nouvelles lignes
    t_before = datetime.datetime.now()
    count_before = _count_fire_pdu(engine, t_before)
    _info(f"Lignes dis.FirePdu avant le test : {count_before} (depuis {t_before.strftime('%H:%M:%S')})")

    # Envoi des FirePdu en UDP
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    base_ts = int(time.time()) & 0xFFFFFFFF
    t_send_start = time.monotonic()

    for i in range(1, count + 1):
        pkt = build_fire_pdu(i, (base_ts + i) & 0xFFFFFFFF)
        sock.sendto(pkt, ("localhost", cfg.PORT))

    sock.close()
    elapsed_send = time.monotonic() - t_send_start
    _ok(f"{count} FirePdu envoyes en UDP en {elapsed_send*1000:.0f}ms")

    # Attendre le traitement Kafka -> SQL
    _info(f"Attente du pipeline ({wait_seconds}s)...")
    time.sleep(wait_seconds)

    # Verification SQL
    count_after = _count_fire_pdu(engine, t_before)
    received = count_after

    _info(f"Lignes dis.FirePdu trouvees : {received}")

    if received >= count:
        _ok(f"Pipeline OK : {received}/{count} lignes en SQL")
        return True
    elif received > 0:
        _warn(f"Pipeline partiel : {received}/{count} lignes recues -- peut-etre besoin d'attendre plus")
        return False
    else:
        _fail("Aucune ligne inseree en SQL -- verifiez que kafka_main.py tourne")
        return False


# ---------------------------------------------------------------------------
# TEST 4 -- Charge et throughput
# ---------------------------------------------------------------------------

def test_4_load(count: int = 50_000, wait_factor: float = 3.0) -> bool:
    """
    Envoie N messages le plus vite possible et verifie zero perte.
    kafka_main.py doit tourner en parallele.
    """
    _section(f"TEST 4 -- Charge (N={count:,} messages)")
    _info("PREREQUIS : kafka_main.py doit tourner en parallele")

    try:
        engine = _get_sql_engine(cfg.DB_NAME)
    except Exception as exc:
        _fail(f"SQL inaccessible : {exc}")
        return False

    t_before = datetime.datetime.now()

    # Envoi UDP en rafale (aucun delai)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    base_ts = int(time.time()) & 0xFFFFFFFF
    t_start = time.monotonic()

    for i in range(1, count + 1):
        pkt = build_fire_pdu(i % 65535 or 1, (base_ts + i) & 0xFFFFFFFF)
        sock.sendto(pkt, ("localhost", cfg.PORT))

    sock.close()
    t_send = time.monotonic() - t_start
    send_rate = count / t_send if t_send > 0 else 0
    _ok(f"{count:,} messages envoyes en {t_send:.2f}s -- {send_rate:.0f} msg/s")

    # Attente proportionnelle au nombre de messages
    wait_time = max(30, t_send * wait_factor)
    _info(f"Attente traitement ({wait_time:.0f}s)...")
    time.sleep(wait_time)

    # Verification
    received = _count_fire_pdu(engine, t_before)
    loss = count - received
    loss_pct = (loss / count * 100) if count > 0 else 0

    _info(f"Envoyes  : {count:,}")
    _info(f"En SQL   : {received:,}")
    _info(f"Perdus   : {loss:,} ({loss_pct:.2f}%)")

    total_time = t_send + wait_time
    throughput = received / total_time if total_time > 0 else 0
    _info(f"Throughput end-to-end : {throughput:.0f} msg/s")

    if loss == 0:
        _ok("Zero perte -- TEST PASSE")
        return True
    elif loss_pct < 1.0:
        _warn(f"Perte < 1% ({loss_pct:.2f}%) -- acceptable mais a investiguer")
        return False
    else:
        _fail(f"Perte significative : {loss_pct:.2f}%")
        return False


# ---------------------------------------------------------------------------
# TEST 5 -- Resilience
# ---------------------------------------------------------------------------

def test_5_resilience(count_per_batch: int = 1000) -> bool:
    """
    Verifie que le pipeline survit a la perte d'un consumer.
    NOTE : Ce test ne peut pas tuer automatiquement un process externe.
    Il s'agit d'un guide de test manuel.
    """
    _section("TEST 5 -- Resilience (guide de test manuel)")
    _info("Ce test necessite une intervention manuelle.")
    print()
    print("  Procedure :")
    print(f"  1. Envoyez {count_per_batch} messages (ce script le fait)")
    print("  2. Dans le Task Manager ou un terminal, tuez un 'KafkaConsumer' process")
    print("  3. Attendez 10 secondes (Kafka rebalance)")
    print(f"  4. Envoyez {count_per_batch} messages de plus (ce script le fait)")
    print(f"  5. Verifiez : {count_per_batch * 2} lignes en SQL")
    print()

    try:
        engine = _get_sql_engine(cfg.DB_NAME)
    except Exception as exc:
        _fail(f"SQL inaccessible : {exc}")
        return False

    t_before = datetime.datetime.now()

    # Batch 1
    _info(f"Envoi du batch 1 ({count_per_batch} messages)...")
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    base_ts = int(time.time()) & 0xFFFFFFFF
    for i in range(1, count_per_batch + 1):
        pkt = build_fire_pdu(i, (base_ts + i) & 0xFFFFFFFF)
        sock.sendto(pkt, ("localhost", cfg.PORT))
    sock.close()
    _ok(f"Batch 1 envoye ({count_per_batch} messages)")

    # Pause pour l'action manuelle
    input(f"\n  >> Tuez maintenant un process 'KafkaConsumer-X' puis appuyez sur Entree...\n")

    _info("Attente du rebalancing Kafka (10s)...")
    time.sleep(10)

    # Batch 2
    _info(f"Envoi du batch 2 ({count_per_batch} messages)...")
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    base_ts2 = (int(time.time()) + 10000) & 0xFFFFFFFF
    for i in range(1, count_per_batch + 1):
        pkt = build_fire_pdu(i, (base_ts2 + i) & 0xFFFFFFFF)
        sock.sendto(pkt, ("localhost", cfg.PORT))
    sock.close()
    _ok(f"Batch 2 envoye ({count_per_batch} messages)")

    _info("Attente traitement (30s)...")
    time.sleep(30)

    # Verification
    received = _count_fire_pdu(engine, t_before)
    expected = count_per_batch * 2

    _info(f"Attendus : {expected:,}")
    _info(f"En SQL   : {received:,}")

    if received >= expected:
        _ok(f"Resilience OK : {received}/{expected} messages preserves")
        return True
    else:
        _warn(f"Resultat partiel : {received}/{expected} -- le rebalancing peut prendre plus de temps")
        return False


# ---------------------------------------------------------------------------
# Runner principal
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Tests end-to-end pipeline Kafka DIS")
    parser.add_argument(
        "--test", type=int, default=0,
        help="Numero du test a executer (0 = tous, 1-5 = test specifique)"
    )
    parser.add_argument(
        "--count", type=int, default=20,
        help="Nombre de messages pour Test 3 et 4 (defaut: 20)"
    )
    parser.add_argument(
        "--wait", type=int, default=15,
        help="Secondes d'attente apres envoi pour Test 3 (defaut: 15)"
    )
    args = parser.parse_args()

    print("\n" + "="*60)
    print("  Tests Pipeline Kafka DIS")
    print(f"  Bootstrap : {cfg.KAFKA_BOOTSTRAP}")
    print(f"  Topic     : {cfg.KAFKA_TOPIC}")
    print(f"  Base SQL  : {cfg.DB_NAME}")
    print(f"  ExerciseID: {cfg.EXERCISE_ID}")
    print("="*60)

    results = {}

    tests_to_run = [args.test] if args.test != 0 else [1, 2, 3, 4]
    # Test 5 est manuel, on l'inclut seulement si demande explicitement
    if args.test == 5:
        tests_to_run = [5]

    for t in tests_to_run:
        if t == 1:
            results[1] = test_1_kafka_connectivity()
        elif t == 2:
            results[2] = test_2_roundtrip()
        elif t == 3:
            results[3] = test_3_pipeline(count=args.count, wait_seconds=args.wait)
        elif t == 4:
            results[4] = test_4_load(count=args.count)
        elif t == 5:
            results[5] = test_5_resilience()
        else:
            print(f"Test {t} inconnu (1-5)")

    # Rapport final
    _section("RAPPORT FINAL")
    total = len(results)
    passed = sum(1 for v in results.values() if v)
    failed = total - passed

    for test_num, result in results.items():
        status = PASS if result else FAIL
        print(f"  {status} Test {test_num}")

    print()
    print(f"  Resultat : {passed}/{total} tests passes")
    if failed > 0:
        print(f"  {failed} test(s) echoue(s)")
        sys.exit(1)
    else:
        print("  Tous les tests sont passes !")
        sys.exit(0)


if __name__ == "__main__":
    main()
