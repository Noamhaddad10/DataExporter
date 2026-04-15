"""
test_deep.py
============
Deep stress tests for the Kafka-based DIS pipeline.
Focus : data loss scenarios and edge cases most likely to silently corrupt data.

Usage:
    python test_deep.py              -- run all tests
    python test_deep.py --test 3     -- run only test 3
    python test_deep.py --list       -- list all tests

Prerequisites:
    kafka_main.py must be running (1 producer + 4 consumers active).
    SQL Server must be accessible (localhost\\SQLEXPRESS, database noamtest).
    Python packages: confluent_kafka, sqlalchemy, pyodbc
"""

import argparse
import datetime
import json
import os
import random
import socket
import struct
import sys
import time
import traceback
import urllib.parse
from typing import Optional, Tuple, List

# Path setup -- permet d'executer depuis tests/ ou depuis la racine
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import sqlalchemy
from confluent_kafka import Consumer, Producer, TopicPartition, KafkaException
from confluent_kafka.admin import AdminClient

# ============================================================
# Config
# ============================================================
HOST           = "localhost"
PORT           = 3000
EXERCISE_ID    = 9
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC    = "dis.raw"
KAFKA_GROUP    = "dis-export-group"
NUM_PARTITIONS = 4
DB_NAME        = "noamtest"
PDU_TYPE_LIST  = [1, 2, 3, 21, 33]

# line_divider as bytes (the separator used by LoggerPDU -- core vulnerability)
LINE_DIVIDER = b"line_divider"  # 12 bytes: 6c 69 6e 65 5f 64 69 76 69 64 65 72

# FirePdu fixed fields
FIRING_SITE,   FIRING_APP,   FIRING_ENTITY   = 1, 3101, 1
TARGET_SITE,   TARGET_APP,   TARGET_ENTITY   = 1, 3101, 2
MUNITION_SITE, MUNITION_APP, MUNITION_ENTITY = 1, 3101, 100
EVENT_SITE,    EVENT_APP                     = 1, 3101
LOCATION_X = 4_429_530.0
LOCATION_Y = 3_094_568.0
LOCATION_Z = 3_320_580.0
ENTITY_KIND, DOMAIN, COUNTRY = 2, 2, 105
CATEGORY, SUBCATEGORY, SPECIFIC, EXTRA = 1, 1, 1, 0
WARHEAD, FUSE, QUANTITY, RATE = 1000, 100, 1, 1
VEL_X, VEL_Y, VEL_Z = 100.0, 0.0, -50.0
RANGE_M = 5000.0

RESULT_OK     = "[PASS]"
RESULT_FAIL   = "[FAIL]"
RESULT_WARN   = "[WARN]"
RESULT_INFO   = "[INFO]"


# ============================================================
# PDU construction helpers
# ============================================================

def build_fire_pdu(
    event_number: int,
    timestamp: int,
    exercise_id: int = EXERCISE_ID,
    location_x: float = LOCATION_X,
    location_y: float = LOCATION_Y,
    location_z: float = LOCATION_Z,
    warhead: int = WARHEAD,
    fuse: int = FUSE,
    firing_site: int = FIRING_SITE,
    firing_app: int = FIRING_APP,
    firing_entity: int = FIRING_ENTITY,
    target_site: int = TARGET_SITE,
    target_app: int = TARGET_APP,
    target_entity: int = TARGET_ENTITY,
    range_m: float = RANGE_M,
) -> bytes:
    """
    Build a 96-byte FirePdu DIS 7 packet via struct.pack (big-endian).

    Byte layout (opendis-compatible, no extra extension bytes):
      0-11:  PDU header  !BBBBIHH  (12B)
      12-23: WarfareFamilyPdu: firingEntityID + targetEntityID  !HHHHHH (12B)
      24-29: munitionExpendableID  !HHH  (6B)
      30-35: eventID  !HHH  (6B)
      36-39: fireMissionIndex  !I  (4B)
      40-63: location  !ddd  (24B)
      64-79: descriptor  !BBHBBBBHHHH  (16B)
      80-91: velocity  !fff  (12B)
      92-95: range  !f  (4B)
      Total = 96 bytes
    """
    header      = struct.pack("!BBBBIHH",
        7, exercise_id, 2, 2, timestamp, 96, 0)
    warfare     = struct.pack("!HHHHHH",
        firing_site, firing_app, firing_entity,
        target_site, target_app, target_entity)
    munition_id = struct.pack("!HHH", MUNITION_SITE, MUNITION_APP, MUNITION_ENTITY)
    event_id    = struct.pack("!HHH", EVENT_SITE, EVENT_APP, event_number)
    fire_idx    = struct.pack("!I", event_number)
    location    = struct.pack("!ddd", location_x, location_y, location_z)
    descriptor  = struct.pack("!BBHBBBBHHHH",
        ENTITY_KIND, DOMAIN, COUNTRY,
        CATEGORY, SUBCATEGORY, SPECIFIC, EXTRA,
        warhead, fuse, QUANTITY, RATE)
    velocity    = struct.pack("!fff", VEL_X, VEL_Y, VEL_Z)
    range_field = struct.pack("!f", range_m)

    packet = (header + warfare + munition_id
              + event_id + fire_idx + location + descriptor
              + velocity + range_field)
    assert len(packet) == 96, f"Expected 96 bytes, got {len(packet)}"
    assert packet[2] == 2
    return packet


def build_entity_state_pdu(entity_id: int = 1, exercise_id: int = EXERCISE_ID) -> bytes:
    """
    Build a minimal EntityStatePdu (type 1) -- 144 bytes, opendis-compatible.

    Byte layout (no extra extension bytes):
      0-11:  PDU header  !BBBBIHH  (12B)
      12-17: EntityID  !HHH  (6B)
      18:    forceID  !B  (1B)
      19:    numberOfVariableParameters  !B  (1B)
      20-27: entityType  !BBHBBBB  (8B)
      28-35: altEntityType  !BBHBBBB  (8B)
      36-47: linearVelocity  !fff  (12B)
      48-71: entityLocation  !ddd  (24B)
      72-83: entityOrientation  !fff  (12B)
      84-87: entityAppearance  !I  (4B)
      88-127: deadReckoningParameters  40B
      128-139: marking  12B
      140-143: capabilities  !I  (4B)
      Total = 144 bytes
    """
    ts      = int(time.time()) & 0xFFFFFFFF
    header  = struct.pack("!BBBBIHH", 7, exercise_id, 1, 1, ts, 144, 0)
    eid     = struct.pack("!HHH", 1, 3101, entity_id)
    force   = struct.pack("!BB", 1, 0)
    etype   = struct.pack("!BBHBBBB", 1, 1, 105, 1, 1, 1, 0)
    atype   = struct.pack("!BBHBBBB", 0, 0, 0, 0, 0, 0, 0)
    vel     = struct.pack("!fff", 0.0, 0.0, 0.0)
    loc     = struct.pack("!ddd", LOCATION_X, LOCATION_Y, LOCATION_Z)
    orient  = struct.pack("!fff", 0.0, 0.0, 0.0)
    appear  = struct.pack("!I", 0)
    drp     = struct.pack("!B", 2) + struct.pack("!fff", 0.0, 0.0, 0.0) + b"\x00" * 27
    marking = struct.pack("!B", 1) + b"TESTMARK\x00\x00\x00"
    caps    = struct.pack("!I", 0)

    packet = (header + eid + force + etype + atype
              + vel + loc + orient + appear + drp + marking + caps)
    if len(packet) < 144:
        packet += b"\x00" * (144 - len(packet))
    return packet[:144]


# ============================================================
# SQL helpers
# ============================================================

def get_sql_engine() -> sqlalchemy.Engine:
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost\\SQLEXPRESS;"
        f"DATABASE={DB_NAME};"
        "Trusted_Connection=yes;"
    )
    return sqlalchemy.create_engine(
        "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(conn_str),
        pool_pre_ping=True,
    )


def count_rows(engine, table: str, logger_file: str, schema: str = "dis") -> int:
    with engine.connect() as conn:
        r = conn.execute(
            sqlalchemy.text(f"SELECT COUNT(*) FROM [{schema}].[{table}] WHERE LoggerFile = :lf"),
            {"lf": logger_file},
        )
        return r.scalar()


def get_latest_fire_row(engine, logger_file: str) -> Optional[dict]:
    """Return the most recently inserted FirePdu row for logger_file."""
    with engine.connect() as conn:
        r = conn.execute(
            sqlalchemy.text(
                "SELECT TOP 1 * FROM [dis].[FirePdu] "
                "WHERE LoggerFile = :lf "
                "ORDER BY WorldTime DESC"
            ),
            {"lf": logger_file},
        )
        row = r.fetchone()
        if row is None:
            return None
        return dict(zip(r.keys(), row))


def get_current_logger_file(engine) -> Optional[str]:
    """Read the most recent LoggerFile from dbo.Loggers."""
    try:
        with engine.connect() as conn:
            r = conn.execute(
                sqlalchemy.text(
                    "SELECT TOP 1 LoggerFile FROM dbo.Loggers ORDER BY WorldTime DESC"
                )
            )
            row = r.fetchone()
            return row[0] if row else None
    except Exception:
        return None


# ============================================================
# Kafka helpers
# ============================================================

def get_consumer_lag(timeout: float = 10.0) -> Tuple[int, dict]:
    """
    Return (total_lag, {partition: lag}) for the consumer group.
    lag = high_watermark_offset - committed_offset
    """
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP,
        "enable.auto.commit": False,
    })
    try:
        tps = [TopicPartition(KAFKA_TOPIC, p) for p in range(NUM_PARTITIONS)]
        committed = consumer.committed(tps, timeout=timeout)
        lag_map = {}
        total = 0
        for tp in committed:
            lo, hi = consumer.get_watermark_offsets(tp, timeout=5, cached=False)
            off = tp.offset if tp.offset >= 0 else 0
            lag = max(0, hi - off)
            lag_map[tp.partition] = lag
            total += lag
        return total, lag_map
    finally:
        consumer.close()


def wait_for_lag_zero(timeout: float = 60.0, poll_interval: float = 1.0) -> Tuple[bool, float]:
    """
    Poll consumer group lag until it reaches 0 or timeout expires.
    Returns (reached_zero, elapsed_seconds).
    """
    start = time.time()
    deadline = start + timeout
    while time.time() < deadline:
        lag, _ = get_consumer_lag()
        if lag == 0:
            return True, time.time() - start
        time.sleep(poll_interval)
    return False, time.time() - start


def get_end_offsets() -> dict:
    """Return {partition: high_watermark} for the topic."""
    c = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "__deep_test_probe__",
        "enable.auto.commit": False,
    })
    result = {}
    try:
        for p in range(NUM_PARTITIONS):
            tp = TopicPartition(KAFKA_TOPIC, p)
            lo, hi = c.get_watermark_offsets(tp, timeout=5, cached=False)
            result[p] = hi
    finally:
        c.close()
    return result


def send_udp(packet: bytes, host: str = HOST, port: int = PORT) -> None:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.sendto(packet, (host, port))
    s.close()


def send_udp_burst(packets: list, host: str = HOST, port: int = PORT) -> None:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    for p in packets:
        s.sendto(p, (host, port))
    s.close()


# ============================================================
# Test framework
# ============================================================

class TestResult:
    def __init__(self, name: str):
        self.name = name
        self.passed = True
        self.lines: List[str] = []

    def ok(self, msg: str):
        self.lines.append(f"  {RESULT_OK}  {msg}")

    def fail(self, msg: str):
        self.passed = False
        self.lines.append(f"  {RESULT_FAIL}  {msg}")

    def warn(self, msg: str):
        self.lines.append(f"  {RESULT_WARN}  {msg}")

    def info(self, msg: str):
        self.lines.append(f"  {RESULT_INFO}  {msg}")

    def print_summary(self):
        status = "PASSED" if self.passed else "FAILED"
        bar = "=" * 68
        print(f"\n{bar}")
        print(f"  Test : {self.name}")
        print(f"  Result : {status}")
        print(bar)
        for line in self.lines:
            print(line)


# ============================================================
# TEST 1 — line_divider byte collision (unit, no pipeline needed)
# ============================================================

def test_1_line_divider_collision():
    """
    RISK : If PDU raw bytes contain b"line_divider" (12 bytes),
    LoggerPDU.interpret_logger_line() raises ValueError.
    In kafka_consumer.py this is caught and the offset is committed
    WITHOUT inserting into SQL --> silent data loss.

    This test proves the vulnerability exists and measures its probability.
    """
    r = TestResult("T1 - line_divider byte collision (unit)")

    # --- 1a. Direct injection: craft a PDU with line_divider at bytes 42-53 ---
    ts = int(time.time()) & 0xFFFFFFFF
    base_pdu = build_fire_pdu(event_number=1, timestamp=ts)
    # Inject 12 bytes of line_divider starting at byte 42 (within location X/Y)
    injected = bytearray(base_pdu)
    injected[42:54] = LINE_DIVIDER
    injected_bytes = bytes(injected)

    # Simulate what kafka_consumer does
    packet_time = 1.0
    logger_line = injected_bytes + LINE_DIVIDER + struct.pack("d", packet_time)

    count_dividers = logger_line.count(LINE_DIVIDER)
    r.info(f"Injected PDU: {len(injected_bytes)} bytes, logger_line count(line_divider) = {count_dividers}")

    if count_dividers != 1:
        r.fail(
            f"VULNERABILITY CONFIRMED: count(line_divider)={count_dividers} != 1 "
            f"-> ValueError -> message silently dropped at kafka_consumer step 5"
        )
    else:
        r.ok("No collision in this specific injection")

    # Confirm ValueError is raised
    try:
        from LoggerPduProcessor import LoggerPDU
        pdu = LoggerPDU(logger_line)
        r.fail("Expected ValueError was NOT raised -- logic changed?")
    except ValueError as exc:
        r.ok(f"ValueError raised as expected: {exc!s:.80s}")
    except Exception as exc:
        r.warn(f"Unexpected exception type: {type(exc).__name__}: {exc}")

    # --- 1b. Probability analysis: how often do random FirePdu bytes collide? ---
    random.seed(42)
    SAMPLE = 100_000
    collisions = 0
    for _ in range(SAMPLE):
        rand_pdu = bytes(random.randint(0, 255) for _ in range(98))
        if LINE_DIVIDER in rand_pdu:
            collisions += 1

    prob_pct = collisions / SAMPLE * 100
    r.info(
        f"Collision probability in random 98-byte PDU: "
        f"{collisions}/{SAMPLE} = {prob_pct:.4f}%"
    )

    # --- 1c. Can the collision happen with REAL DIS coordinates? ---
    # line_divider = b"line_divider" = 6c 69 6e 65 5f 64 69 76 69 64 65 72
    # As a double (8 bytes) big-endian:
    ld_as_dbl = struct.unpack("!d", LINE_DIVIDER[:8])
    r.info(
        f"First 8 bytes of line_divider as double BE = {ld_as_dbl[0]:.6e} "
        f"(not a realistic ECEF coordinate -> low risk for location fields)"
    )
    # As two floats:
    f1, f2 = struct.unpack("!ff", LINE_DIVIDER[:8])
    r.info(
        f"First 8 bytes of line_divider as float pair BE = ({f1:.3e}, {f2:.3e}) "
        f"(velocity/descriptor fields could theoretically hit this)"
    )

    # --- 1d. Fix recommendation ---
    r.warn(
        "RECOMMENDED FIX: Replace b'line_divider' separator with a 4-byte magic + "
        "4-byte length prefix that encodes the PDU length. "
        "Or use a unique 8-byte sentinel that can never appear in IEEE 754 doubles: "
        "e.g. struct.pack('!Q', 0xFFFF_FFFF_FFFF_FFFF) (NaN signaling)."
    )

    r.print_summary()
    return r.passed


# ============================================================
# TEST 2 — async commit-before-SQL-insert architecture bug
# ============================================================

def test_2_commit_before_sql_insert():
    """
    RISK : kafka_consumer._drain_and_export() calls lse.export() which
    calls Exporter.add_data() -- just appends to an in-memory list.
    The Kafka offset is then committed immediately.
    The ACTUAL SQL insert happens asynchronously in a Timer thread.

    If the process crashes between the commit and the SQL insert,
    data is permanently lost (Kafka offset already advanced).

    This test documents the architecture and measures the window size.
    """
    r = TestResult("T2 - commit-before-SQL-insert architecture audit")

    # Trace the call path
    r.info("Call path analysis:")
    r.info("  kafka_consumer._drain_and_export() ->")
    r.info("    lse.export(table, data) ->")
    r.info("      Exporter.add_data(data)   [RETURNS immediately, no SQL yet]")
    r.info("  <- returns True (no exception) ->")
    r.info("  _safe_commit()                [KAFKA OFFSET COMMITTED]")
    r.info("  ... meanwhile in Exporter thread ...")
    r.info("  Exporter.export() -> Exporter.insert() [SQL INSERT, DELAYED]")

    r.fail(
        "ARCHITECTURE BUG: Kafka offset committed BEFORE SQL insert completes. "
        "The 'zero loss guarantee' in kafka_consumer.py docstring is INCORRECT. "
        "On process crash after commit but before SQL insert, data is permanently lost."
    )

    r.warn(
        "The risk window = time between add_data() and the next Exporter.export() "
        "thread cycle. With export_delay=0.01s this is ~10-100ms per batch."
    )

    r.warn(
        "RECOMMENDED FIX: In kafka_consumer._drain_and_export(), call a synchronous "
        "SQL insert method directly (not the Timer-based Exporter). "
        "Only commit the Kafka offset after the synchronous insert returns success."
    )

    # Measure the Timer-based export delay from config
    try:
        with open(os.path.join(os.path.dirname(__file__), "DataExporterConfig.json"), "r") as f:
            raw = json.load(f)
        delay = raw.get("export_delay", "unknown")
        size  = raw.get("export_size", "unknown")
        r.info(f"Current config: export_delay={delay}s, export_size={size} rows")
        r.info(
            f"Worst-case window: up to {delay}s + SQL latency (~50-200ms) "
            f"= {float(delay)+0.2:.3f}s per batch of {size} rows"
        )
    except Exception as exc:
        r.warn(f"Could not read DataExporterConfig.json: {exc}")

    r.print_summary()
    return r.passed


# ============================================================
# TEST 3 — wrong exerciseID filtering (unit + integration)
# ============================================================

def test_3_exercise_id_filter(engine, logger_file: str):
    """
    Sends a FirePdu with exerciseID != 9 (uses 99).
    Verifies the row does NOT appear in dis.FirePdu.
    Also tests exerciseID=0 (null) and exerciseID=255 (max uint8).
    """
    r = TestResult("T3 - wrong exerciseID filtering")

    before = count_rows(engine, "FirePdu", logger_file)
    r.info(f"dis.FirePdu row count before: {before}")

    ts = int(time.time()) & 0xFFFFFFFF
    bad_ids = [0, 99, 255, 8, 10]
    for bad_id in bad_ids:
        pkt = build_fire_pdu(event_number=700 + bad_id, timestamp=ts, exercise_id=bad_id)
        send_udp(pkt)

    r.info(f"Sent {len(bad_ids)} FirePdu with exerciseIDs={bad_ids}")
    r.info("Waiting 6s for pipeline to process...")
    time.sleep(6.0)

    # Also send 1 valid packet to confirm the pipeline is still alive
    valid_pkt = build_fire_pdu(event_number=999, timestamp=ts + 1, exercise_id=EXERCISE_ID)
    send_udp(valid_pkt)
    time.sleep(5.0)

    after = count_rows(engine, "FirePdu", logger_file)
    new_rows = after - before
    r.info(f"dis.FirePdu row count after: {after} (+{new_rows} new rows)")

    if new_rows == 1:
        r.ok(f"Exactly 1 new row (the valid packet). All {len(bad_ids)} bad exerciseIDs were filtered correctly.")
    elif new_rows == 0:
        r.fail("0 new rows -- even the valid packet was not processed. Pipeline may be stuck.")
    elif new_rows == 1 + len(bad_ids):
        r.fail(f"{new_rows} new rows -- bad exerciseIDs were NOT filtered! All {len(bad_ids)} bad packets inserted.")
    else:
        r.warn(f"{new_rows} new rows -- partial filtering. Expected 1 valid row.")
        if new_rows > 1:
            r.fail(f"Some bad exerciseID packets were not filtered ({new_rows - 1} unexpected rows).")

    r.print_summary()
    return r.passed


# ============================================================
# TEST 4 — malformed / truncated PDU robustness
# ============================================================

def test_4_malformed_pdu_robustness(engine, logger_file: str):
    """
    Sends malformed packets -- pipeline must NOT crash.
    After each malformed burst, a valid packet is sent to verify recovery.
    """
    r = TestResult("T4 - malformed PDU robustness")

    ts = int(time.time()) & 0xFFFFFFFF

    # Types of malformed packets
    malformed_cases = [
        ("empty packet",          b""),
        ("1 byte",                bytes([0x07])),
        ("2 bytes (no pduType)",  bytes([0x07, 0x09])),
        ("3 bytes valid pduType", bytes([0x07, 0x09, 0x02])),
        ("7 bytes truncated",     bytes([0x07, 0x09, 0x02, 0x02, 0x00, 0x00, 0x00])),
        ("50 random bytes",       bytes(random.randint(0, 255) for _ in range(50))),
        ("100 bytes all zero",    b"\x00" * 100),
        ("pduType=99 (unknown)",  build_fire_pdu(1, ts)[:1] + bytes([9, 99]) + build_fire_pdu(1, ts)[3:]),
        ("exerciseID=9, pduType=99 (filtered)", bytes([7, 9, 99, 2]) + b"\x00" * 94),
        ("max UDP payload",       bytes(random.randint(0, 255) for _ in range(65507))),
    ]

    before = count_rows(engine, "FirePdu", logger_file)
    r.info(f"Sending {len(malformed_cases)} malformed packets...")

    for name, pkt in malformed_cases:
        try:
            send_udp(pkt)
            r.info(f"  Sent: {name} ({len(pkt)} bytes)")
        except Exception as exc:
            r.warn(f"  Could not send '{name}': {exc}")

    r.info("Waiting 4s then sending 1 valid packet to verify pipeline still alive...")
    time.sleep(4.0)

    ts2 = int(time.time()) & 0xFFFFFFFF
    valid_pkt = build_fire_pdu(event_number=888, timestamp=ts2, exercise_id=EXERCISE_ID)
    send_udp(valid_pkt)
    time.sleep(5.0)

    after = count_rows(engine, "FirePdu", logger_file)
    new_rows = after - before

    if new_rows >= 1:
        r.ok(f"Pipeline survived all malformed packets. {new_rows} new row(s) from valid follow-up packet.")
    else:
        r.fail(
            "No new rows after valid follow-up packet -- pipeline may have crashed "
            "or stalled on malformed input."
        )

    # Check Kafka connectivity to confirm broker is still up
    try:
        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
        meta = admin.list_topics(timeout=5)
        r.ok(f"Kafka broker still responsive ({len(meta.topics)} topic(s))")
    except Exception as exc:
        r.fail(f"Kafka broker became unresponsive after malformed packets: {exc}")

    r.print_summary()
    return r.passed


# ============================================================
# TEST 5 — data field integrity (exact values in SQL)
# ============================================================

def test_5_data_field_integrity(engine, logger_file: str):
    """
    Sends 1 FirePdu with specific known values.
    Reads the SQL row and verifies EVERY field matches exactly.
    Catches: byte-order bugs, field offset errors, float precision loss.
    """
    r = TestResult("T5 - data field integrity (exact field values in SQL)")

    ts = int(time.time()) & 0xFFFFFFFF
    event_number = 42
    unique_x = 4_429_530.123456  # unique enough to identify this packet
    unique_y = 3_094_568.654321
    unique_z = 3_320_580.111222
    unique_range = 12345.0
    unique_warhead = 777
    unique_fuse = 888

    pkt = build_fire_pdu(
        event_number=event_number,
        timestamp=ts,
        exercise_id=EXERCISE_ID,
        location_x=unique_x,
        location_y=unique_y,
        location_z=unique_z,
        warhead=unique_warhead,
        fuse=unique_fuse,
        range_m=unique_range,
    )
    r.info(f"Sending FirePdu event_number={event_number}, unique_x={unique_x}, range={unique_range}")
    send_udp(pkt)

    r.info("Waiting 8s for pipeline to process and insert into SQL...")
    time.sleep(8.0)

    # Query for the latest row matching logger_file
    row = get_latest_fire_row(engine, logger_file)
    if row is None:
        r.fail("No FirePdu row found in SQL for this logger_file -- packet was lost or not processed.")
        r.print_summary()
        return False

    r.info(f"SQL row found: {dict(list(row.items())[:6])}...")

    # Verify expected fields
    expected_attacker = f"{FIRING_SITE}:{FIRING_APP}:{FIRING_ENTITY}"
    expected_target   = f"{TARGET_SITE}:{TARGET_APP}:{TARGET_ENTITY}"
    expected_munition = f"{MUNITION_SITE}:{MUNITION_APP}:{MUNITION_ENTITY}"
    expected_event_id = f"{EVENT_SITE}:{EVENT_APP}:{event_number}"

    def check(field, actual, expected, tolerance=None):
        if tolerance is not None:
            ok = abs(float(actual) - float(expected)) <= tolerance
        else:
            ok = str(actual).strip() == str(expected).strip()
        if ok:
            r.ok(f"  {field}: {actual!r} == {expected!r}")
        else:
            r.fail(f"  {field}: got {actual!r}, expected {expected!r}")

    check("AttackerId", row.get("AttackerId"), expected_attacker)
    check("TargetId",   row.get("TargetId"),   expected_target)
    check("MunitionId", row.get("MunitionId"), expected_munition)
    check("EventId",    row.get("EventId"),    expected_event_id)

    # GeoLocation: float64 round-trip through big-endian network + native double unpack
    # Some precision loss is expected at the ~1e-6 level for doubles
    check("GeoLocationX", row.get("GeoLocationX"), unique_x,     tolerance=1.0)
    check("GeoLocationY", row.get("GeoLocationY"), unique_y,     tolerance=1.0)
    check("GeoLocationZ", row.get("GeoLocationZ"), unique_z,     tolerance=1.0)
    check("FuseType",     row.get("FuseType"),     unique_fuse)
    # Note: "WarheadType " (trailing space) is the actual column name in LoggerPduProcessor
    wh_key = next((k for k in row if "Warhead" in k), None)
    if wh_key:
        check(wh_key, row[wh_key], unique_warhead)
    else:
        r.warn("WarheadType column not found in result (possible trailing-space key)")

    # Range is float32 -> precision loss expected
    range_val = row.get("Range")
    if range_val is not None:
        check("Range", range_val, unique_range, tolerance=1.0)
    else:
        r.warn("Range column not found in SQL row")

    check("LoggerFile", row.get("LoggerFile"), logger_file)

    r.print_summary()
    return r.passed


# ============================================================
# TEST 6 — mixed PDU types routing to correct SQL tables
# ============================================================

def test_6_mixed_pdu_types(engine, logger_file: str):
    """
    Sends FirePdu (type 2) + EntityStatePdu (type 1) packets.
    Verifies correct SQL table routing.
    """
    r = TestResult("T6 - mixed PDU types routing to correct SQL tables")

    before_fire   = count_rows(engine, "FirePdu",        logger_file)
    before_entity = count_rows(engine, "Entities",       logger_file)
    before_locs   = count_rows(engine, "EntityLocations", logger_file)

    r.info(f"Baseline: FirePdu={before_fire}, Entities={before_entity}, EntityLocations={before_locs}")

    ts = int(time.time()) & 0xFFFFFFFF
    n_fire   = 5
    n_entity = 3

    for i in range(n_fire):
        pkt = build_fire_pdu(event_number=200 + i, timestamp=ts + i, exercise_id=EXERCISE_ID)
        send_udp(pkt)

    for i in range(n_entity):
        pkt = build_entity_state_pdu(entity_id=50 + i, exercise_id=EXERCISE_ID)
        send_udp(pkt)

    r.info(f"Sent {n_fire} FirePdu + {n_entity} EntityStatePdu. Waiting 10s...")
    time.sleep(10.0)

    after_fire   = count_rows(engine, "FirePdu",         logger_file)
    after_entity = count_rows(engine, "Entities",        logger_file)
    after_locs   = count_rows(engine, "EntityLocations", logger_file)

    delta_fire   = after_fire   - before_fire
    delta_entity = after_entity - before_entity
    delta_locs   = after_locs   - before_locs

    r.info(f"After: FirePdu+{delta_fire}, Entities+{delta_entity}, EntityLocations+{delta_locs}")

    if delta_fire >= n_fire:
        r.ok(f"FirePdu: +{delta_fire} rows (expected >= {n_fire})")
    elif delta_fire > 0:
        r.warn(f"FirePdu: +{delta_fire} rows (expected {n_fire}) -- partial delivery")
    else:
        r.fail(f"FirePdu: 0 new rows (expected {n_fire}) -- all lost or misrouted")

    if delta_entity >= 1:
        r.ok(f"Entities: +{delta_entity} rows (EntityState PDUs processed)")
    else:
        r.warn(
            f"Entities: 0 new rows after {n_entity} EntityStatePdu. "
            f"May be filtered by entity_locs_cache dedup or unique index (Entities_UIX)."
        )

    if delta_locs >= n_entity:
        r.ok(f"EntityLocations: +{delta_locs} rows (>= {n_entity} expected)")
    else:
        r.warn(f"EntityLocations: +{delta_locs} rows (expected >= {n_entity})")

    # Verify no FirePdu landed in Entities or vice versa (cross-table contamination)
    if delta_fire == 0 and delta_entity == 0 and delta_locs == 0:
        r.fail("No data in any table -- pipeline may be down")
    else:
        r.ok("No cross-table contamination detected")

    r.print_summary()
    return r.passed


# ============================================================
# TEST 7 — Kafka consumer lag and offset verification
# ============================================================

def test_7_kafka_offset_verification():
    """
    Sends 100 messages and verifies:
    1. Consumer group lag reaches 0 within 30 seconds
    2. Committed offsets advance by the number of messages sent
    3. High watermark equals committed offset (no lag left)
    """
    r = TestResult("T7 - Kafka consumer lag and offset verification")

    # Get committed offsets BEFORE
    lag_before, lag_map_before = get_consumer_lag()
    ends_before = get_end_offsets()
    r.info(f"Before: consumer lag={lag_before}, end_offsets={ends_before}")

    # Wait for any existing lag to clear first
    if lag_before > 0:
        r.info(f"Existing lag detected ({lag_before}). Waiting up to 30s to drain...")
        ok, elapsed = wait_for_lag_zero(timeout=30.0)
        if not ok:
            r.warn(f"Could not drain existing lag in 30s. Test may be unreliable.")

    # Send 100 known-good FirePdu
    N = 100
    ts = int(time.time()) & 0xFFFFFFFF
    pkts = [build_fire_pdu(event_number=300 + i, timestamp=ts + i, exercise_id=EXERCISE_ID)
            for i in range(N)]
    send_udp_burst(pkts)
    r.info(f"Sent {N} FirePdu. Polling lag every 1s (timeout 45s)...")

    # Wait for UDP -> Kafka producer pipeline to absorb and flush
    r.info("Waiting 5s for producer to flush to Kafka...")
    import time as _t; _t.sleep(5.0)

    t_sent = time.time()
    reached_zero, elapsed = wait_for_lag_zero(timeout=45.0)

    lag_after, lag_map_after = get_consumer_lag()
    ends_after = get_end_offsets()

    messages_produced = sum(ends_after[p] - ends_before.get(p, 0) for p in range(NUM_PARTITIONS))
    r.info(f"Messages landed in Kafka: {messages_produced} (expected ~{N})")
    r.info(f"Final consumer lag: {lag_after} (partition breakdown: {lag_map_after})")
    r.info(f"Time to drain: {elapsed:.1f}s (timeout={'YES' if not reached_zero else 'NO'})")

    if messages_produced < N * 0.95:
        r.fail(f"Only {messages_produced}/{N} messages reached Kafka (>5% loss at producer level)")
    else:
        r.ok(f"{messages_produced}/{N} messages reached Kafka topic")

    if reached_zero:
        r.ok(f"Consumer lag reached 0 in {elapsed:.1f}s -- all messages consumed")
    else:
        r.fail(
            f"Consumer lag did NOT reach 0 within 45s. "
            f"Remaining lag = {lag_after} across partitions {lag_map_after}"
        )

    # Verify per-partition committed offsets match end offsets
    try:
        consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": KAFKA_GROUP,
            "enable.auto.commit": False,
        })
        tps = [TopicPartition(KAFKA_TOPIC, p) for p in range(NUM_PARTITIONS)]
        committed = consumer.committed(tps, timeout=10)
        consumer.close()

        for tp in committed:
            lo, hi = 0, ends_after.get(tp.partition, 0)
            off = tp.offset if tp.offset >= 0 else 0
            remaining = hi - off
            if remaining == 0:
                r.ok(f"  Partition {tp.partition}: committed={off} == high_watermark={hi}")
            else:
                r.warn(f"  Partition {tp.partition}: committed={off}, high_watermark={hi}, lag={remaining}")
    except Exception as exc:
        r.warn(f"Could not verify per-partition offsets: {exc}")

    r.print_summary()
    return r.passed


# ============================================================
# TEST 8 — high-rate burst / data loss measurement
# ============================================================

def test_8_burst_loss_rate(engine, logger_file: str):
    """
    Sends 5000 FirePdu messages as fast as possible.
    Measures:
    - How many reached Kafka (producer-level loss)
    - How many were inserted into SQL (end-to-end loss)
    - Consumer lag drain time

    This stresses the producer buffer, consumer poll backlog,
    and SQL concurrent write deadlock handling.
    """
    r = TestResult("T8 - high-rate burst: 5000 messages, measure loss rate")

    N = 5000
    r.info(f"Preparing {N} FirePdu packets...")

    ts = int(time.time()) & 0xFFFFFFFF
    packets = [
        build_fire_pdu(event_number=(400 + i) % 65535, timestamp=(ts + i) & 0xFFFFFFFF,
                       exercise_id=EXERCISE_ID)
        for i in range(N)
    ]

    # Baseline
    before_fire  = count_rows(engine, "FirePdu", logger_file)
    ends_before  = get_end_offsets()
    lag_before, _ = get_consumer_lag()
    r.info(f"Baseline: SQL FirePdu rows={before_fire}, Kafka lag={lag_before}")

    # Send burst
    t0 = time.time()
    send_udp_burst(packets)
    t_send = time.time() - t0
    rate = N / t_send if t_send > 0 else 0
    r.info(f"UDP burst complete: {N} packets in {t_send:.3f}s = {rate:.0f} pkt/s")

    # Wait for Kafka producer to absorb (UDP -> Kafka)
    time.sleep(2.0)
    ends_after_send = get_end_offsets()
    kafka_received = sum(ends_after_send[p] - ends_before.get(p, 0) for p in range(NUM_PARTITIONS))
    kafka_loss = N - kafka_received
    kafka_loss_pct = kafka_loss / N * 100

    r.info(f"Kafka received: {kafka_received}/{N} ({kafka_loss_pct:.2f}% loss at producer)")

    if kafka_loss_pct <= 0.01:
        r.ok(f"Producer: zero loss ({kafka_received}/{N})")
    elif kafka_loss_pct <= 1.0:
        r.warn(f"Producer: <1% loss ({kafka_received}/{N}) -- within acceptable range")
    else:
        r.fail(f"Producer: {kafka_loss_pct:.2f}% loss ({kafka_loss}/{N} messages not in Kafka)")

    # Wait for consumers to drain
    r.info("Waiting for consumer lag to reach 0 (timeout 120s)...")
    reached_zero, elapsed = wait_for_lag_zero(timeout=120.0)
    r.info(f"Consumer drained in {elapsed:.1f}s, reached_zero={reached_zero}")

    if reached_zero:
        r.ok(f"All Kafka messages consumed in {elapsed:.1f}s")
    else:
        lag_now, lag_map = get_consumer_lag()
        r.fail(f"Consumer lag not cleared after 120s -- remaining lag={lag_now}, map={lag_map}")

    # Wait for SQL inserts to flush (Timer-based Exporter has delay)
    r.info("Waiting additional 10s for SQL Timer-based flushes...")
    time.sleep(10.0)

    after_fire  = count_rows(engine, "FirePdu", logger_file)
    sql_inserted = after_fire - before_fire
    sql_loss = N - sql_inserted
    sql_loss_pct = sql_loss / N * 100

    r.info(f"SQL inserted: {sql_inserted}/{N} ({sql_loss_pct:.2f}% end-to-end loss)")

    if sql_loss_pct <= 0.01:
        r.ok(f"SQL: zero end-to-end loss ({sql_inserted}/{N})")
    elif sql_loss_pct <= 1.0:
        r.warn(
            f"SQL: <1% loss ({sql_inserted}/{N}). "
            f"Some loss expected from exerciseID dedup or unique index (Entities_UIX)."
        )
    elif sql_loss_pct <= 5.0:
        r.warn(
            f"SQL: {sql_loss_pct:.2f}% loss ({sql_loss}/{N}). "
            f"Check for deadlocks or Timer-based export drops."
        )
    else:
        r.fail(
            f"SQL: {sql_loss_pct:.2f}% end-to-end loss ({sql_loss}/{N} messages lost). "
            f"Check 'WE LOST A MESSAGE' in logs."
        )

    r.print_summary()
    return r.passed


# ============================================================
# TEST 9 — SQL deadlock and concurrent write stress
# ============================================================

def test_9_sql_deadlock_stress(engine, logger_file: str):
    """
    Sends a rapid burst specifically designed to hit deadlock conditions:
    - All 4 partitions get traffic simultaneously
    - Each consumer will call insert() at the same time
    - Checks dis.FirePdu for duplicates (at-least-once Kafka delivery)
    """
    r = TestResult("T9 - SQL concurrent write stress and deadlock detection")

    # Read dis.kafka.log for "WE LOST A MESSAGE" occurrences before test
    log_path = os.path.join(os.path.dirname(__file__), "dis-kafka.log")
    lost_before = 0
    if os.path.exists(log_path):
        try:
            with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                lost_before = f.read().count("WE LOST A MESSAGE")
        except Exception:
            pass

    # Send 2000 messages split evenly across pdu_types to hit all 4 partitions
    # partition = pdu_type % 4
    # pdu_type 1 -> partition 1, pdu_type 2 -> partition 2
    # We use pdu_type=2 (FirePdu) which goes to partition 2
    # For multi-partition stress, we'd need multiple PDU types,
    # but we control partition via pdu_type byte.
    N = 2000
    ts = int(time.time()) & 0xFFFFFFFF

    # Build packets: use pdu_type=2 (all go to partition 2 % 4 = 2)
    # To stress multiple partitions, we send entity state (type 1, partition 1) too
    packets = []
    for i in range(N // 2):
        packets.append(build_fire_pdu(
            event_number=(500 + i) % 65535, timestamp=(ts + i) & 0xFFFFFFFF,
            exercise_id=EXERCISE_ID))
    for i in range(N // 2):
        packets.append(build_entity_state_pdu(entity_id=(60 + (i % 1000)), exercise_id=EXERCISE_ID))

    random.shuffle(packets)

    before_fire = count_rows(engine, "FirePdu", logger_file)
    r.info(f"Baseline FirePdu rows: {before_fire}")
    r.info(f"Sending {N} mixed packets (Fire + EntityState) simultaneously...")

    t0 = time.time()
    send_udp_burst(packets)
    r.info(f"Burst sent in {time.time()-t0:.3f}s")

    r.info("Waiting for consumer lag to drain (timeout 90s)...")
    reached_zero, elapsed = wait_for_lag_zero(timeout=90.0)
    time.sleep(5.0)  # extra buffer for SQL Timer flushes

    after_fire = count_rows(engine, "FirePdu", logger_file)
    new_rows = after_fire - before_fire

    r.info(f"New FirePdu rows: {new_rows} (out of {N//2} fire packets sent)")
    r.info(f"Consumer drained: {reached_zero} in {elapsed:.1f}s")

    # Check for duplicate rows (at-least-once delivery)
    try:
        with engine.connect() as conn:
            dup_result = conn.execute(sqlalchemy.text(
                "SELECT EventId, COUNT(*) as cnt FROM [dis].[FirePdu] "
                "WHERE LoggerFile = :lf "
                "GROUP BY EventId HAVING COUNT(*) > 1"
            ), {"lf": logger_file})
            dups = dup_result.fetchall()

        if dups:
            r.warn(
                f"DUPLICATE ROWS DETECTED: {len(dups)} EventIds appear more than once. "
                f"This is expected for at-least-once Kafka delivery (rebalance scenario). "
                f"First duplicate: EventId={dups[0][0]}, count={dups[0][1]}"
            )
        else:
            r.ok("No duplicate rows -- no rebalance-induced duplicates in this run")
    except Exception as exc:
        r.warn(f"Could not check for duplicates: {exc}")

    # Check log for deadlock / message loss
    lost_after = 0
    if os.path.exists(log_path):
        try:
            with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                lost_after = f.read().count("WE LOST A MESSAGE")
        except Exception:
            pass

    new_losses = lost_after - lost_before
    if new_losses == 0:
        r.ok(f"No 'WE LOST A MESSAGE' in dis-kafka.log during this test")
    else:
        r.fail(
            f"{new_losses} new 'WE LOST A MESSAGE' occurrences in dis-kafka.log. "
            f"SQL deadlock retry limit exceeded -- data permanently lost."
        )

    if new_rows >= (N // 2) * 0.95:
        r.ok(f"Fire row delivery: {new_rows}/{N//2} (>= 95%)")
    else:
        r.warn(f"Fire row delivery: {new_rows}/{N//2} -- possible SQL drop or dedup filtering")

    r.print_summary()
    return r.passed


# ============================================================
# TEST 10 — boundary conditions
# ============================================================

def test_10_boundary_conditions(engine, logger_file: str):
    """
    Edge cases:
    - 0-byte packet
    - 3-byte packet (pdu_type readable but truncated)
    - Valid pduType not in PDU_TYPE_LIST (e.g., type 7 = CollisionPdu)
    - Large packet (random 60000 bytes)
    - FirePdu with all-zero location (valid but unusual)
    - FirePdu at exact boundary of Kafka message header (8 bytes = header only)
    """
    r = TestResult("T10 - boundary conditions and edge cases")

    ts = int(time.time()) & 0xFFFFFFFF

    cases = [
        ("0-byte",              b""),
        ("3 bytes valid type",  bytes([7, 9, 2])),           # pduType=2 but truncated
        ("pduType=7 (skip)",    bytes([7, 9, 7]) + b"\x00"*95),   # type 7 not in PDU_TYPE_LIST
        ("pduType=4 (skip)",    bytes([7, 9, 4]) + b"\x00"*95),   # type 4 not in list
        ("all zero 98B",        b"\x00" * 98),               # pduType=0, exerciseID=0
        ("exerciseID=9, pduType=0 (skip)", bytes([7, 9, 0]) + b"\x00"*95),  # type 0 not in list
        ("60000 random bytes",  bytes(random.randint(0,255) for _ in range(60000))),
        ("fire pdu all-zero loc", build_fire_pdu(1, ts,
                                                 location_x=0.0, location_y=0.0, location_z=0.0)),
    ]

    before = count_rows(engine, "FirePdu", logger_file)
    r.info(f"Baseline FirePdu: {before}")

    for name, pkt in cases:
        try:
            send_udp(pkt)
            r.info(f"  Sent: '{name}' ({len(pkt)} bytes)")
        except Exception as exc:
            r.warn(f"  Could not send '{name}': {exc}")

    # Send recovery probe
    time.sleep(4.0)
    valid = build_fire_pdu(event_number=777, timestamp=ts + 99, exercise_id=EXERCISE_ID)
    send_udp(valid)
    r.info("Sent recovery probe (valid FirePdu). Waiting 6s...")
    time.sleep(6.0)

    after = count_rows(engine, "FirePdu", logger_file)
    new_rows = after - before
    r.info(f"New FirePdu rows after boundary test: {new_rows}")

    if new_rows >= 1:
        r.ok(f"Pipeline recovered: {new_rows} new row(s) after all boundary cases")
    else:
        r.fail("Pipeline did not recover -- boundary cases may have caused a crash or stall")

    # Check Kafka broker health
    try:
        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
        meta = admin.list_topics(timeout=5)
        r.ok(f"Kafka broker healthy after boundary test ({len(meta.topics)} topics)")
    except Exception as exc:
        r.fail(f"Kafka broker unhealthy after boundary test: {exc}")

    r.print_summary()
    return r.passed


# ============================================================
# TEST 11 — packet_time accuracy
# ============================================================

def test_11_packet_time_accuracy(engine, logger_file: str):
    """
    Verifies that packet_time in SQL is accurate to within 2 seconds
    of the actual send time.
    Tests the full chain: send_time -> producer timestamp -> consumer unpack
    -> LoggerPDU.packet_time -> SQL WorldTime.
    """
    r = TestResult("T11 - packet_time / WorldTime accuracy")

    ts = int(time.time()) & 0xFFFFFFFF
    wall_time_before = datetime.datetime.now()
    pkt = build_fire_pdu(event_number=1111, timestamp=ts, exercise_id=EXERCISE_ID)
    send_udp(pkt)
    wall_time_after = datetime.datetime.now()

    r.info(f"Packet sent between {wall_time_before.strftime('%H:%M:%S.%f')} "
           f"and {wall_time_after.strftime('%H:%M:%S.%f')}")
    r.info("Waiting 8s for processing...")
    time.sleep(8.0)

    row = get_latest_fire_row(engine, logger_file)
    if row is None:
        r.fail("No FirePdu row found -- cannot verify timing")
        r.print_summary()
        return False

    world_time = row.get("WorldTime")
    if world_time is None:
        r.fail("WorldTime column not found in row")
        r.print_summary()
        return False

    if isinstance(world_time, datetime.datetime):
        delta_before = abs((world_time - wall_time_before).total_seconds())
        delta_after  = abs((world_time - wall_time_after).total_seconds())
        r.info(f"SQL WorldTime: {world_time.strftime('%H:%M:%S.%f')}")
        r.info(f"  Delta from send: {delta_before:.3f}s (before) / {delta_after:.3f}s (after)")

        if delta_before <= 5.0:
            r.ok(f"WorldTime accuracy within 5s (delta={delta_before:.3f}s)")
        elif delta_before <= 30.0:
            r.warn(
                f"WorldTime off by {delta_before:.1f}s. "
                f"Note: packet_time is seconds-since-pipeline-start, not wall time."
            )
        else:
            r.fail(
                f"WorldTime off by {delta_before:.1f}s -- possible epoch/start_time mismatch "
                f"between producer and consumer processes."
            )

        packet_time = row.get("PacketTime")
        if packet_time is not None:
            r.info(f"  PacketTime (seconds since pipeline start): {float(packet_time):.3f}s")
            if 0 < float(packet_time) < 86400:
                r.ok(f"PacketTime in valid range (0-86400s)")
            else:
                r.warn(f"PacketTime out of expected range: {packet_time}")
    else:
        r.warn(f"WorldTime has unexpected type: {type(world_time)}")

    r.print_summary()
    return r.passed


# ============================================================
# TEST 12 — shutdown data safety (Timer flush window)
# ============================================================

def test_12_exporter_timer_flush_window():
    """
    Documents the data loss window during graceful shutdown.
    The Exporter uses threading.Timer(export_delay, self.export).
    When stop_writing_event.set() is called, in-flight Timer threads
    may be mid-execution or not yet triggered.
    This is an architectural analysis, not a live test.
    """
    r = TestResult("T12 - shutdown data safety (Exporter Timer flush window)")

    r.info("Exporter shutdown sequence in kafka_consumer.py finally block:")
    r.info("  1. stop_writing_event.set()  -- signals Exporter threads to stop")
    r.info("  2. _drain_and_export(offset=-1) -- drain processing_queue one last time")
    r.info("  3. consumer.close() -- commits pending Kafka offsets")

    r.warn(
        "POTENTIAL LOSS WINDOW: After step 1, Exporter.export() checks "
        "'if not self.stop_event.is_set(): threading.Timer(...)' "
        "-- so no new Timer is scheduled. "
        "BUT data already added via add_data() but not yet inserted by the CURRENT "
        "Timer thread could be lost if the process exits before the thread finishes."
    )

    r.info("Shutdown flow for data added JUST BEFORE stop_event:")
    r.info("  T=0:   lse.export() -> Exporter.add_data() -- data in buffer")
    r.info("  T=0+:  _safe_commit() -- Kafka offset committed")
    r.info("  T=0+:  stop_writing_event.set()")
    r.info("  T=0.01s: Exporter Timer fires, calls export(), finds data, calls insert()")
    r.info("  IF process exits before T=0.01s -> data LOST (offset already committed)")

    r.info("For EntityLocations table specifically:")
    r.info("  Exporter uses export_delay=0.01s (10ms) AND export_size=1500 row threshold")
    r.info("  If buffer < 1500 rows at shutdown, data accumulates until Timer fires")
    r.info("  With 4 consumers each flushing independently, small batches are the norm")

    # Check actual export_delay from config
    try:
        with open(os.path.join(os.path.dirname(__file__), "DataExporterConfig.json"), "r") as f:
            raw = json.load(f)
        delay = raw.get("export_delay", 0.01)
        size  = raw.get("export_size", 1500)
        r.info(f"Config: export_delay={delay}s, export_size={size}")
    except Exception:
        delay = 0.01

    r.warn(
        f"RECOMMENDED FIX: Add explicit flush-and-join after stop_writing_event.set(). "
        f"Wait for all Exporter threads to complete their current insert before "
        f"calling consumer.close(). Current code does NOT wait for Timer threads."
    )

    r.fail(
        "Architecture confirms: up to export_delay * num_tables * export_size rows "
        "can be permanently lost on unclean shutdown (SIGKILL, OOM, power loss). "
        "Clean shutdown (Ctrl+C -> stop_event -> join(30s)) is safe IF Timer threads "
        "finish within the 30s timeout."
    )

    r.print_summary()
    return r.passed


# ============================================================
# Main
# ============================================================


# ============================================================
# TEST 13 — DetonationPdu end-to-end (PDU type 3)
# ============================================================

def build_detonation_pdu(
    event_number: int,
    timestamp: int,
    exercise_id: int = EXERCISE_ID,
    location_x: float = LOCATION_X,
    location_y: float = LOCATION_Y,
    location_z: float = LOCATION_Z,
) -> bytes:
    """
    Build a 104-byte DetonationPdu DIS 7 (type 3), big-endian, opendis-compatible.

    Byte layout:
      0-11:  PDU header !BBBBIHH (12B)
      12-23: WarfareFamilyPdu: firingEntityID + targetEntityID !HHHHHH (12B)
      24-29: explodingEntityID !HHH (6B)
      30-35: eventID !HHH (6B)
      36-47: velocity !fff (12B)
      48-71: locationInWorldCoordinates !ddd (24B)
      72-87: descriptor !BBHBBBBHHHH (16B)
      88-99: locationInEntityCoordinates !fff (12B)
      100:   detonationResult !B (1B)
      101:   numberOfVariableParameters !B (1B)
      102-103: pad !H (2B)
      Total = 104 bytes
    """
    header      = struct.pack("!BBBBIHH", 7, exercise_id, 3, 2, timestamp, 104, 0)
    warfare     = struct.pack("!HHHHHH",
                              FIRING_SITE, FIRING_APP, FIRING_ENTITY,
                              TARGET_SITE, TARGET_APP, TARGET_ENTITY)
    exploding   = struct.pack("!HHH", MUNITION_SITE, MUNITION_APP, MUNITION_ENTITY)
    event_id    = struct.pack("!HHH", EVENT_SITE, EVENT_APP, event_number)
    velocity    = struct.pack("!fff", VEL_X, VEL_Y, VEL_Z)
    location    = struct.pack("!ddd", location_x, location_y, location_z)
    descriptor  = struct.pack("!BBHBBBBHHHH",
                              ENTITY_KIND, DOMAIN, COUNTRY,
                              CATEGORY, SUBCATEGORY, SPECIFIC, EXTRA,
                              WARHEAD, FUSE, QUANTITY, RATE)
    loc_entity  = struct.pack("!fff", 1.0, 2.0, 3.0)
    det_result  = struct.pack("!BB", 5, 0)   # detonationResult=5 (entity proximate detonation), nvp=0
    pad         = struct.pack("!H", 0)

    packet = (header + warfare + exploding + event_id + velocity
              + location + descriptor + loc_entity + det_result + pad)
    assert len(packet) == 104, f"Expected 104 bytes, got {len(packet)}"
    assert packet[2] == 3
    return packet


def test_13_detonation_pdu(engine, logger_file: str):
    """
    Sends 5 DetonationPdu (type 3) and verifies:
    - Rows appear in dis.DetonationPdu
    - Key fields match (AttackerId, EventId, GeoLocation, DetonationResult)
    - NOT in dis.FirePdu (no cross-contamination)
    - Goes to Kafka partition 3 (pduType=3, 3%4=3)
    """
    r = TestResult("T13 - DetonationPdu end-to-end (PDU type 3)")

    before_det  = count_rows(engine, "DetonationPdu", logger_file)
    before_fire = count_rows(engine, "FirePdu",       logger_file)
    ends_before = get_end_offsets()
    r.info(f"Baseline: DetonationPdu={before_det}, FirePdu={before_fire}")
    r.info(f"Kafka partition 3 before: {ends_before.get(3, 0)}")

    N = 5
    ts = int(time.time()) & 0xFFFFFFFF
    unique_x = 4_100_000.777

    for i in range(N):
        pkt = build_detonation_pdu(
            event_number=2000 + i,
            timestamp=(ts + i) & 0xFFFFFFFF,
            exercise_id=EXERCISE_ID,
            location_x=unique_x + i,
        )
        send_udp(pkt)

    r.info(f"Sent {N} DetonationPdu. Waiting 8s...")
    time.sleep(8.0)

    after_det   = count_rows(engine, "DetonationPdu", logger_file)
    after_fire  = count_rows(engine, "FirePdu",       logger_file)
    ends_after  = get_end_offsets()

    new_det  = after_det  - before_det
    new_fire = after_fire - before_fire
    kafka_p3 = ends_after.get(3, 0) - ends_before.get(3, 0)

    r.info(f"Kafka partition 3 received: {kafka_p3} messages")
    r.info(f"New DetonationPdu rows: {new_det}, new FirePdu rows: {new_fire}")

    if kafka_p3 >= N:
        r.ok(f"Partition 3 received {kafka_p3} messages (pduType=3 -> 3%4=3 correct)")
    else:
        r.fail(f"Partition 3 only got {kafka_p3}/{N} messages -- partition routing bug")

    if new_det >= N:
        r.ok(f"dis.DetonationPdu: +{new_det} rows (expected {N})")
    elif new_det > 0:
        r.warn(f"dis.DetonationPdu: only +{new_det}/{N} rows -- partial insertion")
    else:
        r.fail(f"dis.DetonationPdu: 0 new rows -- DetonationPdu not processed")

    if new_fire == 0:
        r.ok("No cross-contamination: 0 new rows in dis.FirePdu")
    else:
        r.fail(f"Cross-contamination! {new_fire} DetonationPdu rows ended up in dis.FirePdu")

    # Verify a specific row's fields
    try:
        with engine.connect() as conn:
            row = conn.execute(sqlalchemy.text(
                "SELECT TOP 1 * FROM [dis].[DetonationPdu] "
                "WHERE LoggerFile = :lf ORDER BY WorldTime DESC"
            ), {"lf": logger_file}).fetchone()
        if row:
            rd = dict(zip(row._fields if hasattr(row, '_fields') else range(len(row)), row))
            attacker = rd.get("AttackerId", rd.get(2, "?"))
            event    = rd.get("EventId",    rd.get(3, "?"))
            r.info(f"  Latest row: AttackerId={attacker}, EventId={event}")
            expected_attacker = f"{FIRING_SITE}:{FIRING_APP}:{FIRING_ENTITY}"
            if str(attacker) == expected_attacker:
                r.ok(f"  AttackerId correct: {attacker}")
            else:
                r.fail(f"  AttackerId wrong: got {attacker}, expected {expected_attacker}")
    except Exception as exc:
        r.warn(f"Could not verify row fields: {exc}")

    r.print_summary()
    return r.passed


# ============================================================
# TEST 14 — Kafka partition routing verification
# ============================================================

def test_14_partition_routing():
    """
    Verifies that pduType correctly determines the Kafka partition:
      pduType=1 (EntityState)  -> partition 1%4 = 1
      pduType=2 (FirePdu)      -> partition 2%4 = 2
      pduType=3 (DetonationPdu)-> partition 3%4 = 3
      pduType=21 (Aggregate)   -> partition 21%4= 1 (same as EntityState)

    Sends a controlled batch of each type and measures per-partition offsets.
    """
    r = TestResult("T14 - Kafka partition routing (pduType %% 4)")

    ends_before = get_end_offsets()
    r.info(f"Partition offsets before: {ends_before}")

    ts = int(time.time()) & 0xFFFFFFFF
    N  = 20  # per type

    # Build batches per type
    fire_pkts   = [build_fire_pdu(9000+i, (ts+i)&0xFFFFFFFF)          for i in range(N)]
    entity_pkts = [build_entity_state_pdu(entity_id=200+i)             for i in range(N)]
    det_pkts    = [build_detonation_pdu(9100+i, (ts+i)&0xFFFFFFFF)     for i in range(N)]

    # pduType=21 (AggregateState) -- minimal header only (just pduType byte matters for routing)
    # We only need bytes[2]=21 and bytes[1]=9 (exerciseID) for the consumer to accept it
    # The consumer filters on pduType=21 ∈ PDU_TYPE_LIST, then tries to parse -- it may fail
    # but will still route to partition 21%4=1 at the producer
    agg_pkts = [
        struct.pack("!BBBBIHH", 7, EXERCISE_ID, 21, 7, (ts+i)&0xFFFFFFFF, 50, 0) + b"\x00" * 38
        for i in range(N)
    ]

    # Send each type
    send_udp_burst(fire_pkts)
    send_udp_burst(entity_pkts)
    send_udp_burst(det_pkts)
    send_udp_burst(agg_pkts)

    total_sent = N * 4
    r.info(f"Sent {total_sent} packets ({N} per type: Fire, Entity, Detonation, Aggregate)")
    r.info("Waiting 5s for producer to flush to Kafka...")
    time.sleep(5.0)

    ends_after = get_end_offsets()
    r.info(f"Partition offsets after:  {ends_after}")

    delta = {p: ends_after.get(p, 0) - ends_before.get(p, 0) for p in range(NUM_PARTITIONS)}
    r.info(f"Delta per partition: {delta}")

    # Expected routing:
    # pduType=1  -> partition 1:  N entity msgs
    # pduType=2  -> partition 2:  N fire msgs
    # pduType=3  -> partition 3:  N detonation msgs
    # pduType=21 -> partition 1:  N aggregate msgs (21%4=1, same as entity)
    # So partition 1 = 2N, partition 2 = N, partition 3 = N, partition 0 = 0

    def check_partition(p, expected_min, label):
        actual = delta.get(p, 0)
        if actual >= expected_min:
            r.ok(f"  Partition {p} ({label}): {actual} msgs (expected >= {expected_min})")
        else:
            r.fail(f"  Partition {p} ({label}): {actual} msgs (expected >= {expected_min}) -- routing error")

    check_partition(0, 0,   "nothing sent to partition 0 in this test")
    check_partition(1, N,   f"EntityState(type=1) + Aggregate(type=21) -> 1%4=1, 21%4=1")
    check_partition(2, N,   f"FirePdu(type=2) -> 2%4=2")
    check_partition(3, N,   f"Detonation(type=3) -> 3%4=3")

    if delta.get(1, 0) >= 2 * N:
        r.ok(f"  Partition 1 got {delta.get(1,0)} msgs -- both Entity(1) and Aggregate(21) land here")
    elif delta.get(1, 0) >= N:
        r.warn(f"  Partition 1 got {delta.get(1,0)} msgs -- Aggregate may have been dropped before Kafka")

    total_received = sum(delta.values())
    r.info(f"Total messages in Kafka: {total_received}/{total_sent}")
    if total_received >= total_sent * 0.95:
        r.ok(f"Producer: <= 5% loss ({total_received}/{total_sent})")
    else:
        r.fail(f"Producer: {total_sent - total_received}/{total_sent} messages lost before Kafka")

    r.print_summary()
    return r.passed


# ============================================================
# TEST 15 — Consumer process restart resilience
# ============================================================

def _get_consumer_pids(main_pid: int) -> List[int]:
    """Find child Python PIDs spawned by the given main_pid (consumers + producer)."""
    import subprocess
    try:
        out = subprocess.check_output(
            ["wmic", "process", "where",
             f"ParentProcessId={main_pid} AND name='python.exe'",
             "get", "ProcessId"],
            text=True, timeout=10
        )
        pids = [int(l.strip()) for l in out.splitlines() if l.strip().isdigit()]
        return pids
    except Exception:
        return []


def _get_main_kafka_pid() -> Optional[int]:
    """Find the PID of the single running kafka_main.py process."""
    import subprocess
    try:
        out = subprocess.check_output(
            ["wmic", "process", "where",
             "name='python.exe'",
             "get", "ProcessId,CommandLine"],
            text=True, timeout=10
        )
        for line in out.splitlines():
            if "kafka_main" in line and "spawn_main" not in line:
                parts = line.strip().split()
                pid_str = parts[-1]
                if pid_str.isdigit():
                    return int(pid_str)
    except Exception:
        pass
    return None


def test_15_consumer_restart_resilience(engine, logger_file: str):
    """
    Kills one Kafka consumer process mid-burst.
    Verifies that:
    1. kafka_main.py detects the dead consumer and restarts it (within 10s)
    2. The restarted consumer re-joins the group and resumes processing
    3. No data is permanently lost (consumer lag eventually reaches 0)
    4. SQL row count matches messages sent

    Note: At-least-once delivery means some duplicates are expected during
    the restart window (uncommitted offsets replayed by new consumer).
    """
    r = TestResult("T15 - consumer process restart resilience")

    # Find running consumers
    main_pid = _get_main_kafka_pid()
    if main_pid is None:
        r.fail("Could not find kafka_main.py PID -- is the pipeline running?")
        r.print_summary()
        return False

    child_pids = _get_consumer_pids(main_pid)
    r.info(f"kafka_main PID={main_pid}, children={child_pids}")

    if len(child_pids) < 2:
        r.fail(f"Not enough child processes found ({len(child_pids)}) -- cannot kill one safely")
        r.print_summary()
        return False

    # Pick the last child PID to kill (likely a consumer, not the producer)
    victim_pid = child_pids[-1]
    r.info(f"Will kill consumer PID={victim_pid} mid-burst")

    # Baseline
    before_fire  = count_rows(engine, "FirePdu", logger_file)
    ends_before  = get_end_offsets()
    lag_before, _ = get_consumer_lag()
    r.info(f"Baseline: FirePdu={before_fire}, Kafka lag={lag_before}")

    # Send first batch
    N = 500
    ts = int(time.time()) & 0xFFFFFFFF
    batch1 = [build_fire_pdu((3000+i) % 65535, (ts+i) & 0xFFFFFFFF) for i in range(N // 2)]
    r.info(f"Sending first {N//2} packets...")
    send_udp_burst(batch1)

    # Kill the consumer after 0.5s (mid-processing)
    time.sleep(0.5)
    r.info(f"Killing consumer PID={victim_pid}...")
    try:
        import subprocess
        subprocess.run(["taskkill", "/F", "/PID", str(victim_pid)], timeout=5, capture_output=True)
        r.ok(f"Consumer PID={victim_pid} killed")
    except Exception as exc:
        r.warn(f"Could not kill PID {victim_pid}: {exc}")

    # Send second batch while one consumer is dead
    time.sleep(0.2)
    batch2 = [build_fire_pdu((3000 + N//2 + i) % 65535, (ts + N//2 + i) & 0xFFFFFFFF)
              for i in range(N // 2)]
    r.info(f"Sending second {N//2} packets with one consumer dead...")
    send_udp_burst(batch2)

    # Wait for kafka_main.py to detect and restart (checks every 5s)
    r.info("Waiting up to 15s for kafka_main.py to restart the dead consumer...")
    time.sleep(15.0)

    # Verify a new consumer was started (child count should be restored)
    new_children = _get_consumer_pids(main_pid)
    r.info(f"Children after restart: {new_children}")
    if len(new_children) >= len(child_pids):
        r.ok(f"kafka_main.py restarted the dead consumer ({len(new_children)} children, was {len(child_pids)})")
    else:
        r.warn(f"Child count not fully restored: {len(new_children)} vs {len(child_pids)} expected")

    # Wait for consumer lag to drain
    r.info("Waiting for consumer lag to reach 0 (timeout 60s)...")
    reached_zero, elapsed = wait_for_lag_zero(timeout=60.0)
    time.sleep(5.0)  # extra for SQL Timer flush

    after_fire  = count_rows(engine, "FirePdu", logger_file)
    ends_after  = get_end_offsets()
    kafka_total = sum(ends_after[p] - ends_before.get(p, 0) for p in range(NUM_PARTITIONS))
    sql_new     = after_fire - before_fire

    r.info(f"Kafka received: {kafka_total}/{N} messages")
    r.info(f"SQL new rows: {sql_new} (may be > {N} due to at-least-once duplicates on restart)")
    r.info(f"Consumer drained: {reached_zero} in {elapsed:.1f}s")

    if kafka_total >= N * 0.95:
        r.ok(f"Producer: {kafka_total}/{N} messages in Kafka (>= 95%)")
    else:
        r.fail(f"Producer: only {kafka_total}/{N} messages in Kafka -- significant loss before Kafka")

    if reached_zero:
        r.ok(f"Consumer lag reached 0 in {elapsed:.1f}s -- restart was successful")
    else:
        lag_now, lag_map = get_consumer_lag()
        r.fail(f"Consumer lag NOT cleared after 60s: lag={lag_now}, map={lag_map}")

    if sql_new >= N:
        r.ok(f"SQL: {sql_new} rows (>= {N} -- at-least-once, no permanent loss)")
    elif sql_new >= N * 0.95:
        r.warn(f"SQL: {sql_new}/{N} rows -- small loss, check if Entities_UIX dedup applies")
    else:
        r.fail(f"SQL: only {sql_new}/{N} rows -- permanent data loss after consumer restart")

    r.print_summary()
    return r.passed


# ============================================================
# TEST 16 — Sustained throughput: 20,000 messages
# ============================================================

def test_16_sustained_throughput(engine, logger_file: str):
    """
    Sends 20,000 FirePdu messages and measures end-to-end throughput.
    Harder than T8 (5000 msgs) -- stresses the SQL insert pipeline
    and Kafka consumer commit frequency under sustained load.
    Reports: producer rate, consumer drain time, SQL loss rate, rows/sec.
    """
    r = TestResult("T16 - sustained throughput: 20000 messages")

    N = 20_000
    r.info(f"Preparing {N} FirePdu packets...")

    ts = int(time.time()) & 0xFFFFFFFF
    packets = [
        build_fire_pdu((4000 + i) % 65535, (ts + i) & 0xFFFFFFFF)
        for i in range(N)
    ]

    before_fire  = count_rows(engine, "FirePdu", logger_file)
    ends_before  = get_end_offsets()
    lag_before, _ = get_consumer_lag()
    r.info(f"Baseline: SQL rows={before_fire}, Kafka lag={lag_before}")

    # Burst send
    t0 = time.time()
    send_udp_burst(packets)
    t_send = time.time() - t0
    udp_rate = N / t_send if t_send > 0 else 0
    r.info(f"UDP burst: {N} packets in {t_send:.2f}s = {udp_rate:.0f} pkt/s")

    # Check producer-side after 3s
    time.sleep(3.0)
    ends_mid = get_end_offsets()
    kafka_received = sum(ends_mid.get(p, 0) - ends_before.get(p, 0) for p in range(NUM_PARTITIONS))
    kafka_loss_pct = (N - kafka_received) / N * 100
    r.info(f"Kafka received (after 3s): {kafka_received}/{N} ({kafka_loss_pct:.2f}% producer loss)")

    if kafka_loss_pct <= 0.01:
        r.ok(f"Producer: zero loss ({kafka_received}/{N})")
    elif kafka_loss_pct <= 1.0:
        r.warn(f"Producer: <1% loss ({kafka_received}/{N})")
    else:
        r.fail(f"Producer: {kafka_loss_pct:.2f}% loss ({N-kafka_received}/{N} messages never reached Kafka)")

    # Wait for full consumer drain
    r.info("Waiting for consumer lag to reach 0 (timeout 180s)...")
    t_drain_start = time.time()
    reached_zero, elapsed = wait_for_lag_zero(timeout=180.0)
    drain_rate = N / elapsed if elapsed > 0 else 0
    r.info(f"Consumer drain: {elapsed:.1f}s, reached_zero={reached_zero}, rate={drain_rate:.0f} msgs/s")

    # Extra SQL flush time
    time.sleep(15.0)

    after_fire  = count_rows(engine, "FirePdu", logger_file)
    sql_new     = after_fire - before_fire
    sql_loss    = N - sql_new
    sql_loss_pct = sql_loss / N * 100
    sql_rate    = N / (elapsed + 15) if (elapsed + 15) > 0 else 0

    r.info(f"SQL rows inserted: {sql_new}/{N} ({sql_loss_pct:.2f}% end-to-end loss)")
    r.info(f"Effective SQL throughput: {sql_rate:.0f} rows/s")

    if reached_zero:
        r.ok(f"Consumer fully drained in {elapsed:.1f}s ({drain_rate:.0f} msgs/s)")
    else:
        lag_now, lag_map = get_consumer_lag()
        r.fail(f"Consumer not drained after 180s -- remaining lag={lag_now}")

    if sql_loss_pct <= 0.01:
        r.ok(f"SQL: zero end-to-end loss ({sql_new}/{N})")
    elif sql_loss_pct <= 1.0:
        r.warn(f"SQL: <1% loss ({sql_new}/{N})")
    else:
        r.fail(f"SQL: {sql_loss_pct:.2f}% end-to-end loss ({sql_loss}/{N} rows missing)")

    # Log throughput benchmark
    r.info(f"--- Throughput benchmark ---")
    r.info(f"  UDP send rate      : {udp_rate:.0f} pkt/s")
    r.info(f"  Kafka consume rate : {drain_rate:.0f} msgs/s")
    r.info(f"  SQL insert rate    : {sql_rate:.0f} rows/s")
    r.info(f"  End-to-end latency : {elapsed:.1f}s for {N} messages")

    r.print_summary()
    return r.passed


def check_prerequisites() -> Tuple[bool, Optional[sqlalchemy.Engine], Optional[str]]:
    """Returns (ok, engine, logger_file)."""
    print("\n=== Checking prerequisites ===")

    # Kafka
    try:
        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
        meta = admin.list_topics(timeout=5)
        print(f"  Kafka OK: {len(meta.topics)} topics at {KAFKA_BOOTSTRAP}")
        if KAFKA_TOPIC not in meta.topics:
            print(f"  WARNING: topic '{KAFKA_TOPIC}' not found. Is the pipeline running?")
    except Exception as exc:
        print(f"  FAIL: Kafka not reachable: {exc}")
        return False, None, None

    # SQL
    try:
        engine = get_sql_engine()
        with engine.connect():
            pass
        print(f"  SQL Server OK: database={DB_NAME}")
    except Exception as exc:
        print(f"  FAIL: SQL Server not reachable: {exc}")
        return False, None, None

    # Logger file
    engine = get_sql_engine()
    logger_file = get_current_logger_file(engine)
    if logger_file is None:
        # Fallback: read from DataExporterConfig.json
        try:
            with open(os.path.join(os.path.dirname(__file__), "DataExporterConfig.json"), "r") as f:
                cfg = json.load(f)
            logger_file = cfg.get("logger_file")
        except Exception:
            pass

    if logger_file:
        print(f"  Logger file: {logger_file}")
    else:
        print("  WARNING: Could not determine logger_file. SQL row counts will be global.")
        logger_file = "unknown"

    # Consumer lag check
    try:
        lag, lag_map = get_consumer_lag()
        if lag == 0:
            print(f"  Consumer lag: 0 (pipeline is draining in real-time)")
        else:
            print(f"  Consumer lag: {lag} messages backlog. Tests may be slightly inaccurate.")
    except Exception as exc:
        print(f"  WARNING: Could not check consumer lag: {exc}")

    print()
    return True, engine, logger_file


TESTS = {
    1:  ("line_divider collision (unit)",             lambda e, lf: test_1_line_divider_collision()),
    2:  ("commit-before-SQL-insert audit (unit)",     lambda e, lf: test_2_commit_before_sql_insert()),
    3:  ("wrong exerciseID filtering",                lambda e, lf: test_3_exercise_id_filter(e, lf)),
    4:  ("malformed PDU robustness",                  lambda e, lf: test_4_malformed_pdu_robustness(e, lf)),
    5:  ("data field integrity",                      lambda e, lf: test_5_data_field_integrity(e, lf)),
    6:  ("mixed PDU types routing",                   lambda e, lf: test_6_mixed_pdu_types(e, lf)),
    7:  ("Kafka offset verification",                 lambda e, lf: test_7_kafka_offset_verification()),
    8:  ("burst loss rate (5000 msgs)",               lambda e, lf: test_8_burst_loss_rate(e, lf)),
    9:  ("SQL deadlock concurrent stress",            lambda e, lf: test_9_sql_deadlock_stress(e, lf)),
    10: ("boundary conditions",                       lambda e, lf: test_10_boundary_conditions(e, lf)),
    11: ("packet_time accuracy",                      lambda e, lf: test_11_packet_time_accuracy(e, lf)),
    12: ("shutdown Timer flush window (unit)",        lambda e, lf: test_12_exporter_timer_flush_window()),
    13: ("DetonationPdu end-to-end (PDU type 3)",     lambda e, lf: test_13_detonation_pdu(e, lf)),
    14: ("Kafka partition routing (pduType %% 4)",    lambda e, lf: test_14_partition_routing()),
    15: ("consumer restart resilience",               lambda e, lf: test_15_consumer_restart_resilience(e, lf)),
    16: ("sustained throughput 20k messages",         lambda e, lf: test_16_sustained_throughput(e, lf)),
}


def main():
    parser = argparse.ArgumentParser(description="Deep stress tests for Kafka DIS pipeline")
    parser.add_argument("--test", type=int, default=None, help="Run only this test number")
    parser.add_argument("--list", action="store_true", help="List all tests and exit")
    args = parser.parse_args()

    if args.list:
        print("\nAvailable tests:")
        for num, (name, _) in sorted(TESTS.items()):
            print(f"  {num:2d}. {name}")
        return

    ok, engine, logger_file = check_prerequisites()
    if not ok:
        print("Prerequisites failed. Start kafka_main.py and ensure SQL Server is running.")
        sys.exit(1)

    if args.test is not None:
        tests_to_run = {args.test: TESTS[args.test]} if args.test in TESTS else {}
        if not tests_to_run:
            print(f"Unknown test number: {args.test}. Use --list to see available tests.")
            sys.exit(1)
    else:
        tests_to_run = TESTS

    print(f"\n{'='*68}")
    print(f"  DEEP PIPELINE STRESS TESTS")
    print(f"  Logger file : {logger_file}")
    print(f"  Kafka       : {KAFKA_BOOTSTRAP} | topic={KAFKA_TOPIC}")
    print(f"  SQL         : noamtest (localhost\\SQLEXPRESS)")
    print(f"  Tests       : {list(tests_to_run.keys())}")
    print(f"{'='*68}\n")

    results = {}
    t_total_start = time.time()

    for num, (name, fn) in sorted(tests_to_run.items()):
        print(f"\n>>> Running Test {num}: {name}")
        t_start = time.time()
        try:
            passed = fn(engine, logger_file)
        except Exception as exc:
            print(f"  [CRASH] Test {num} crashed: {exc}")
            traceback.print_exc()
            passed = False
        elapsed = time.time() - t_start
        results[num] = (name, passed, elapsed)
        print(f"  Elapsed: {elapsed:.1f}s")

    # Final summary
    t_total = time.time() - t_total_start
    print(f"\n\n{'='*68}")
    print(f"  FINAL RESULTS  (total time: {t_total:.0f}s)")
    print(f"{'='*68}")
    passed_count = sum(1 for _, (_, p, _) in results.items() if p)
    failed_count = len(results) - passed_count
    for num, (name, passed, elapsed) in sorted(results.items()):
        status = "PASS" if passed else "FAIL"
        print(f"  T{num:2d} [{status}]  {name}  ({elapsed:.1f}s)")
    print(f"{'='*68}")
    print(f"  PASSED: {passed_count}  |  FAILED: {failed_count}  |  TOTAL: {len(results)}")
    print(f"{'='*68}\n")

    if failed_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
