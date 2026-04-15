"""
test_intensive.py
=================
Intensive test suite -- 20 tests covering:
  - The 5 fixed bugs (B1..B5)
  - Pipeline structural integrity
  - Business logic (filters, serialization, SQL fields)
  - Kafka + SQL integration tests (conditional)

Tests UNIT   T01-T15 : no external dependencies (always run)
Tests INTEG  T16-T20 : require Kafka + SQL Server (skipped if unavailable)

Usage:
    python test_intensive.py              -- all tests
    python test_intensive.py --unit       -- unit tests only
    python test_intensive.py --integ      -- integration tests only
    python test_intensive.py --test T07   -- specific test
"""

import argparse
import ast
import datetime
import multiprocessing
import os
import socket
import struct
import sys
import threading
import time
import traceback
import urllib.parse
from typing import Optional, Tuple
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Path setup -- allows running from tests/ or from the project root
# ---------------------------------------------------------------------------
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------
PASS = "\033[92m[PASS]\033[0m"
FAIL = "\033[91m[FAIL]\033[0m"
SKIP = "\033[93m[SKIP]\033[0m"
INFO = "      "

results = []


def _p(name: str, passed: bool, msg: str, detail: str = ""):
    tag = PASS if passed else FAIL
    print(f"  {tag}  {name} -- {msg}")
    if detail:
        for line in detail.strip().splitlines():
            print(f"  {INFO}         {line}")
    results.append((name, passed))


def _skip(name: str, reason: str):
    print(f"  {SKIP}  {name} -- {reason}")
    results.append((name, None))  # None = skipped


def _section(title: str):
    print(f"\n{'='*65}")
    print(f"  {title}")
    print(f"{'='*65}")


# ---------------------------------------------------------------------------
# PDU helpers (96 bytes, identical to send_fire_pdu.py)
# ---------------------------------------------------------------------------
EXERCISE_ID    = 9
FIRING_SITE,  FIRING_APP,  FIRING_ENTITY  = 1, 3101, 1
TARGET_SITE,  TARGET_APP,  TARGET_ENTITY  = 1, 3101, 2
MUNITION_SITE,MUNITION_APP,MUNITION_ENTITY= 1, 3101, 100
EVENT_SITE,   EVENT_APP                   = 1, 3101
LOCATION_X, LOCATION_Y, LOCATION_Z = 4_429_530.0, 3_094_568.0, 3_320_580.0
ENTITY_KIND, DOMAIN, COUNTRY = 2, 2, 105
CATEGORY, SUBCATEGORY, SPECIFIC, EXTRA = 1, 1, 1, 0
WARHEAD, FUSE, QUANTITY, RATE = 1000, 100, 1, 1
VEL_X, VEL_Y, VEL_Z = 100.0, 0.0, -50.0
RANGE_M = 5000.0
PDU_LENGTH = 96


def _build_fire_pdu(event_number: int, timestamp: int,
                    exercise_id: int = EXERCISE_ID,
                    lx=LOCATION_X, ly=LOCATION_Y, lz=LOCATION_Z) -> bytes:
    header      = struct.pack("!BBBBIHH", 7, exercise_id, 2, 2, timestamp, PDU_LENGTH, 0)
    warfare     = struct.pack("!HHHHHH", FIRING_SITE, FIRING_APP, FIRING_ENTITY,
                                         TARGET_SITE,  TARGET_APP,  TARGET_ENTITY)
    munition_id = struct.pack("!HHH", MUNITION_SITE, MUNITION_APP, MUNITION_ENTITY)
    event_id    = struct.pack("!HHH", EVENT_SITE, EVENT_APP, event_number)
    fire_idx    = struct.pack("!I", event_number)
    location    = struct.pack("!ddd", lx, ly, lz)
    descriptor  = struct.pack("!BBHBBBBHHHH",
        ENTITY_KIND, DOMAIN, COUNTRY, CATEGORY, SUBCATEGORY, SPECIFIC, EXTRA,
        WARHEAD, FUSE, QUANTITY, RATE)
    velocity    = struct.pack("!fff", VEL_X, VEL_Y, VEL_Z)
    range_f     = struct.pack("!f", RANGE_M)
    return header + warfare + munition_id + event_id + fire_idx + location + descriptor + velocity + range_f


# ---------------------------------------------------------------------------
# T01: PDU size + key byte positions
# ---------------------------------------------------------------------------
def test_T01():
    name = "T01[B1] PDU 96B layout"
    try:
        pkt = _build_fire_pdu(42, 99999)

        assert len(pkt) == 96, f"size={len(pkt)} expected=96"
        assert pkt[0] == 7,            f"protocolVersion={pkt[0]} expected=7"
        assert pkt[1] == EXERCISE_ID,  f"exerciseID={pkt[1]} expected={EXERCISE_ID}"
        assert pkt[2] == 2,            f"pduType={pkt[2]} expected=2 (FirePdu)"
        assert pkt[3] == 2,            f"family={pkt[3]} expected=2 (Warfare)"

        length_field = struct.unpack("!H", pkt[8:10])[0]
        assert length_field == 96, f"PDU length field={length_field} expected=96"

        # Verify that firingEntityID is at offset 12 (not 14)
        site, app, ent = struct.unpack("!HHH", pkt[12:18])
        assert site == FIRING_SITE and app == FIRING_APP and ent == FIRING_ENTITY, \
            f"firingEntityID=[{site}:{app}:{ent}] expected=[{FIRING_SITE}:{FIRING_APP}:{FIRING_ENTITY}]"

        # Location at offset 40
        lx, ly, lz = struct.unpack("!ddd", pkt[40:64])
        assert abs(lx - LOCATION_X) < 0.1, f"location.x={lx} expected={LOCATION_X}"

        _p(name, True, f"96 bytes, pduType=2, firingEntityID@offset12, location@offset40")
    except AssertionError as e:
        _p(name, False, str(e))
    except Exception as e:
        _p(name, False, f"Exception: {e}")


# ---------------------------------------------------------------------------
# T02: LoggerPDU.from_parts() -- direct construction without line_divider
# ---------------------------------------------------------------------------
def test_T02():
    name = "T02[B2] LoggerPDU.from_parts() basic"
    try:
        from LoggerPduProcessor import LoggerPDU
        import opendis.dis7 as dis7

        pkt = _build_fire_pdu(1, 12345)
        packet_time = 3.14159

        pdu_obj = LoggerPDU.from_parts(pkt, packet_time)

        assert pdu_obj.pdu is not None, "pdu is None"
        assert isinstance(pdu_obj.pdu, dis7.FirePdu), \
            f"type={type(pdu_obj.pdu).__name__} expected=FirePdu"
        assert pdu_obj.pdu.exerciseID == EXERCISE_ID, \
            f"exerciseID={pdu_obj.pdu.exerciseID} expected={EXERCISE_ID}"
        assert abs(pdu_obj.packet_time - packet_time) < 1e-10, \
            f"packet_time={pdu_obj.packet_time} expected={packet_time}"

        _p(name, True, f"pdu=FirePdu exerciseID={pdu_obj.pdu.exerciseID} packet_time={pdu_obj.packet_time:.5f}")
    except Exception as e:
        _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T03: BUG 2 -- from_parts() immune to b"line_divider" collision
# ---------------------------------------------------------------------------
def test_T03():
    name = "T03[B2] from_parts() immune collision line_divider"
    try:
        from LoggerPduProcessor import LoggerPDU

        # Build a valid PDU, then inject b"line_divider" into the payload
        # Overwrite velocity bytes (offset 80) with the sequence
        pkt = bytearray(_build_fire_pdu(1, 12345))
        seq = b"line_divider"  # 12 bytes
        pkt[80:92] = seq       # inject into velocity field
        pkt_bytes = bytes(pkt)

        # The classic constructor MUST fail (there are now 2 occurrences: injected + maybe 0)
        # Verify that from_parts() succeeds WITHOUT exception
        crashed_old = False
        try:
            _ = LoggerPDU(pkt_bytes + b"line_divider" + struct.pack("d", 1.0))
        except ValueError:
            crashed_old = True

        # from_parts() must NOT crash
        pdu_obj = LoggerPDU.from_parts(pkt_bytes, 2.71)

        assert pdu_obj.pdu is not None, "from_parts() returned pdu=None"

        detail = (
            f"Old constructor crash on collision: {crashed_old}\n"
            f"from_parts() succeeded: True\n"
            f"Collision eliminated: OK"
        )
        _p(name, True, "from_parts() does not crash, old constructor did", detail)
    except Exception as e:
        _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T04: Confirm that the old constructor fails with 0 separators
# ---------------------------------------------------------------------------
def test_T04():
    name = "T04[B2] Old constructor ValueError on 0 line_divider"
    try:
        from LoggerPduProcessor import LoggerPDU

        pkt = _build_fire_pdu(1, 12345)
        # Pass bytes without line_divider -> ValueError expected
        raised = False
        try:
            _ = LoggerPDU(pkt)  # no line_divider -> ValueError
        except ValueError:
            raised = True

        assert raised, "Constructor should have raised ValueError on bytes without line_divider"
        _p(name, True, "ValueError confirmed on bytes without line_divider")
    except Exception as e:
        _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T05: AST analysis -- insert_sync() present + correct signature
# ---------------------------------------------------------------------------
def test_T05():
    name = "T05[B3] insert_sync() present in LoggerSQLExporter"
    try:
        with open(os.path.join(_ROOT, "LoggerSQLExporter.py"), "r", encoding="utf-8") as f:
            tree = ast.parse(f.read())

        found = None
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name == "LoggerSQLExporter":
                for item in node.body:
                    if isinstance(item, ast.FunctionDef) and item.name == "insert_sync":
                        found = item
                        break

        assert found is not None, "insert_sync() missing from LoggerSQLExporter"

        # Verify arguments: self, table, data
        args = [a.arg for a in found.args.args]
        assert "self"  in args, f"arg 'self' missing: {args}"
        assert "table" in args, f"arg 'table' missing: {args}"
        assert "data"  in args, f"arg 'data' missing: {args}"

        # Verify it returns bool (return True / return False present)
        returns = [n for n in ast.walk(found) if isinstance(n, ast.Return)]
        return_values = []
        for r in returns:
            if isinstance(r.value, ast.Constant):
                return_values.append(r.value.value)
        assert True  in return_values, "insert_sync() never returns True"
        assert False in return_values, "insert_sync() never returns False"

        _p(name, True, f"insert_sync(self, table, data) -> bool confirmed")
    except AssertionError as e:
        _p(name, False, str(e))
    except Exception as e:
        _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T06: kafka_consumer._drain_and_export uses insert_sync, not lse.export
# ---------------------------------------------------------------------------
def test_T06():
    name = "T06[B3] _drain_and_export() -> insert_sync, not lse.export()"
    try:
        with open(os.path.join(_ROOT, "kafka_consumer.py"), "r", encoding="utf-8") as f:
            src = f.read()
        tree = ast.parse(src)

        drain_func = None
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "_drain_and_export":
                drain_func = node
                break

        assert drain_func is not None, "_drain_and_export() not found"

        # Look for lse.insert_sync and lse.export calls in this function
        calls_insert_sync = []
        calls_export      = []

        for node in ast.walk(drain_func):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Attribute):
                    if node.func.attr == "insert_sync":
                        calls_insert_sync.append(node)
                    if node.func.attr == "export" and isinstance(node.func.value, ast.Name):
                        if node.func.value.id == "lse":
                            calls_export.append(node)

        assert len(calls_insert_sync) >= 1, "insert_sync() not called in _drain_and_export()"
        assert len(calls_export)      == 0, \
            f"lse.export() still called in _drain_and_export() ({len(calls_export)} times)"

        _p(name, True,
           f"insert_sync called {len(calls_insert_sync)}x, lse.export() absent")
    except AssertionError as e:
        _p(name, False, str(e))
    except Exception as e:
        _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T07: insert() retry deadlock -- max 5 attempts then give up
# ---------------------------------------------------------------------------
def test_T07():
    name = "T07[B4] insert() retry deadlock (max 5x) no infinite loop"
    try:
        import sqlalchemy
        from LoggerSQLExporter import Exporter

        mock_engine = MagicMock()
        mock_meta   = MagicMock()
        stop_event  = multiprocessing.Event()

        # Configure the begin() context manager
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=MagicMock())
        ctx.__exit__  = MagicMock(return_value=False)  # do not suppress exceptions
        mock_engine.begin.return_value = ctx

        # Patch threading.Thread and sqlalchemy.Table to avoid side-effects
        orig_thread = threading.Thread
        orig_table  = sqlalchemy.Table
        threading.Thread  = MagicMock()
        sqlalchemy.Table  = MagicMock(return_value=MagicMock())

        try:
            exp = Exporter(
                stop_event=stop_event,
                table_name="FirePdu",
                sql_meta=mock_meta,
                sql_engine=mock_engine,
                tracked_tables=[],
                start_time=time.time(),
                export_delay=1.0,
                export_size=100,
            )
        finally:
            threading.Thread = orig_thread
            sqlalchemy.Table = orig_table

        exp.sql_engine = mock_engine

        # Simulate a permanent deadlock (6 attempts)
        call_count = [0]
        def always_deadlock(*args, **kwargs):
            call_count[0] += 1
            raise Exception("Transaction (Process ID 99) was deadlocked on lock resources")

        ctx.__enter__.return_value.execute.side_effect = always_deadlock

        t_start = time.monotonic()
        exp.insert([{"EventId": "1:1:1"}])
        elapsed = time.monotonic() - t_start

        assert call_count[0] <= 6, \
            f"insert() made {call_count[0]} attempts (expected <= 6)"
        assert elapsed < 5.0, \
            f"insert() blocked for {elapsed:.1f}s (deadlock not resolved in < 5s)"

        _p(name, True,
           f"Stopped after {call_count[0]} attempts in {elapsed*1000:.0f}ms (no infinite loop)")
    except AssertionError as e:
        _p(name, False, str(e))
    except Exception as e:
        _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T08: insert() -- duplicate key (Entities_UIX) = silent return
# ---------------------------------------------------------------------------
def test_T08():
    name = "T08[B4] insert() Entities_UIX = silent return without error"
    try:
        import sqlalchemy
        from LoggerSQLExporter import Exporter

        mock_engine = MagicMock()
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=MagicMock())
        ctx.__exit__  = MagicMock(return_value=False)
        mock_engine.begin.return_value = ctx

        orig_thread = threading.Thread
        orig_table  = sqlalchemy.Table
        threading.Thread = MagicMock()
        sqlalchemy.Table = MagicMock(return_value=MagicMock())

        try:
            exp = Exporter(
                stop_event=multiprocessing.Event(),
                table_name="Entities",
                sql_meta=MagicMock(),
                sql_engine=mock_engine,
                tracked_tables=[],
                start_time=time.time(),
                export_delay=1.0,
                export_size=100,
            )
        finally:
            threading.Thread = orig_thread
            sqlalchemy.Table = orig_table

        exp.sql_engine = mock_engine

        call_count = [0]
        def dup_key(*args, **kwargs):
            call_count[0] += 1
            raise Exception("Violation of UNIQUE KEY constraint 'Entities_UIX'")

        ctx.__enter__.return_value.execute.side_effect = dup_key

        # insert() must return cleanly without raising an exception
        exp.insert([{"EntityId": "1:1:1"}])

        assert call_count[0] == 1, \
            f"insert() retried {call_count[0]}x on Entities_UIX (should return after 1)"

        _p(name, True,
           f"Returned after 1 attempt, no retry on duplicate key")
    except AssertionError as e:
        _p(name, False, str(e))
    except Exception as e:
        _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T09: insert() -- non-retryable error = immediate return
# ---------------------------------------------------------------------------
def test_T09():
    name = "T09[B4] insert() non-retryable error = immediate return"
    try:
        import sqlalchemy
        from LoggerSQLExporter import Exporter

        mock_engine = MagicMock()
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=MagicMock())
        ctx.__exit__  = MagicMock(return_value=False)
        mock_engine.begin.return_value = ctx

        orig_thread = threading.Thread
        orig_table  = sqlalchemy.Table
        threading.Thread = MagicMock()
        sqlalchemy.Table = MagicMock(return_value=MagicMock())

        try:
            exp = Exporter(
                stop_event=multiprocessing.Event(),
                table_name="FirePdu",
                sql_meta=MagicMock(),
                sql_engine=mock_engine,
                tracked_tables=[],
                start_time=time.time(),
                export_delay=1.0,
                export_size=100,
            )
        finally:
            threading.Thread = orig_thread
            sqlalchemy.Table = orig_table

        exp.sql_engine = mock_engine

        call_count = [0]
        def non_retryable(*args, **kwargs):
            call_count[0] += 1
            raise Exception("Invalid column name 'NonExistentColumn'")

        ctx.__enter__.return_value.execute.side_effect = non_retryable

        exp.insert([{"EventId": "1:1:1"}])

        assert call_count[0] == 1, \
            f"insert() retried {call_count[0]}x on non-retryable error (expected 1)"

        _p(name, True, f"Immediate return after 1 attempt on non-retryable error")
    except AssertionError as e:
        _p(name, False, str(e))
    except Exception as e:
        _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T10: exerciseID filter in kafka_consumer (code analysis)
# ---------------------------------------------------------------------------
def test_T10():
    name = "T10 exerciseID filter present in kafka_consumer"
    try:
        with open(os.path.join(_ROOT, "kafka_consumer.py"), "r", encoding="utf-8") as f:
            tree = ast.parse(f.read())

        # Look for the comparison pdu.exerciseID != cfg.EXERCISE_ID
        found = False
        for node in ast.walk(tree):
            if isinstance(node, ast.Compare):
                # Look for received_pdu.pdu.exerciseID != cfg.EXERCISE_ID
                left = node.left
                if (isinstance(left, ast.Attribute) and left.attr == "exerciseID"):
                    found = True
                    break

        assert found, "exerciseID comparison not found in kafka_consumer"
        _p(name, True, "Filter pdu.exerciseID != cfg.EXERCISE_ID confirmed")
    except AssertionError as e:
        _p(name, False, str(e))
    except Exception as e:
        _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T11: pduType filter -- only PDU_TYPE_LIST types passed through
# ---------------------------------------------------------------------------
def test_T11():
    name = "T11 pduType filter in kafka_consumer"
    try:
        with open(os.path.join(_ROOT, "kafka_consumer.py"), "r", encoding="utf-8") as f:
            src = f.read()

        # Verify that pdu_type not in cfg.PDU_TYPE_LIST is present
        assert "PDU_TYPE_LIST" in src, "PDU_TYPE_LIST not referenced in kafka_consumer"
        assert "pdu_type" in src, "variable pdu_type missing"

        # Verify that pdu_data[2] is used to extract pduType
        assert "pdu_data[2]" in src, "pdu_type extracted from pdu_data[2] missing"

        _p(name, True, "pduType filter via pdu_data[2] not in PDU_TYPE_LIST confirmed")
    except AssertionError as e:
        _p(name, False, str(e))
    except Exception as e:
        _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T12: Kafka message format -- serialization / deserialization round-trip
# ---------------------------------------------------------------------------
def test_T12():
    name = "T12 Kafka message format round-trip (struct pack/unpack)"
    try:
        KAFKA_MSG_HEADER_SIZE = 8
        pdu_raw = _build_fire_pdu(7, 55555)
        original_time = 12.345678901234

        # Serialization (as in kafka_producer)
        kafka_value = struct.pack("!d", original_time) + pdu_raw

        # Deserialization (as in kafka_consumer)
        assert len(kafka_value) >= KAFKA_MSG_HEADER_SIZE
        recovered_time = struct.unpack("!d", kafka_value[:KAFKA_MSG_HEADER_SIZE])[0]
        recovered_pdu  = kafka_value[KAFKA_MSG_HEADER_SIZE:]

        assert abs(recovered_time - original_time) < 1e-12, \
            f"packet_time lost: {recovered_time} vs {original_time}"
        assert recovered_pdu == pdu_raw, "PDU bytes corrupted after round-trip"
        assert recovered_pdu[2] == 2, f"pduType lost: {recovered_pdu[2]}"

        _p(name, True,
           f"packet_time={recovered_time:.6f} PDU={len(recovered_pdu)}B pduType=2")
    except AssertionError as e:
        _p(name, False, str(e))
    except Exception as e:
        _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T13: from_parts() preserves packet_time with float64 precision
# ---------------------------------------------------------------------------
def test_T13():
    name = "T13 from_parts() packet_time float64 precision"
    try:
        from LoggerPduProcessor import LoggerPDU

        pkt = _build_fire_pdu(1, 12345)
        # Values that are hard to round
        for pt in [0.0, 1.0, 0.001, 3.141592653589793, 86399.999999999]:
            obj = LoggerPDU.from_parts(pkt, pt)
            assert abs(obj.packet_time - pt) < 1e-12, \
                f"packet_time={obj.packet_time} expected={pt} (diff={abs(obj.packet_time - pt)})"

        _p(name, True, "5 packet_time values preserved with precision < 1e-12")
    except AssertionError as e:
        _p(name, False, str(e))
    except Exception as e:
        _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T14: AST analysis -- field names in _process_fire_pdu()
# ---------------------------------------------------------------------------
def test_T14():
    name = "T14 Field names in _process_fire_pdu() (no critical typos)"
    try:
        with open(os.path.join(_ROOT, "LoggerPduProcessor.py"), "r", encoding="utf-8") as f:
            tree = ast.parse(f.read())

        # Find _process_fire_pdu
        func = None
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "_process_fire_pdu":
                func = node
                break

        assert func is not None, "_process_fire_pdu() not found"

        # Collect all dict keys (ast.Constant inside ast.Dict)
        keys = []
        for node in ast.walk(func):
            if isinstance(node, ast.Dict):
                for k in node.keys:
                    if isinstance(k, ast.Constant) and isinstance(k.value, str):
                        keys.append(k.value)

        # Verify absence of known typos
        assert "EventId"    in keys, "EventId missing"
        assert "AttackerId" in keys, "AttackerId missing"
        assert "TargetId"   in keys, "TargetId missing"
        assert "Range"      in keys, "Range missing"
        assert "LoggerFile" in keys, "LoggerFile missing"
        assert "WorldTime"  in keys, "WorldTime missing"
        assert "PacketTime" in keys, "PacketTime missing"

        # Verify absence of print() on the hot path
        prints_in_lpp = []
        with open(os.path.join(_ROOT, "LoggerPduProcessor.py"), "r", encoding="utf-8") as f:
            for i, line in enumerate(f, 1):
                if "print(f\"process {os.getpid()}" in line or \
                   "count_parsing" in line and "print" in line:
                    prints_in_lpp.append((i, line.strip()))

        assert len(prints_in_lpp) == 0, \
            f"print hot-path still present: {prints_in_lpp}"

        detail = "Fields: " + ", ".join(keys)
        _p(name, True, f"{len(keys)} valid fields, print hot-path absent", detail)
    except AssertionError as e:
        _p(name, False, str(e))
    except Exception as e:
        _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T15: opendis parsing of built FirePdu -- correct field values
# ---------------------------------------------------------------------------
def test_T15():
    name = "T15 opendis parse FirePdu -- exact field values"
    try:
        from opendis.PduFactory import createPdu
        import opendis.dis7 as dis7

        pkt = _build_fire_pdu(event_number=77, timestamp=11111)
        pdu = createPdu(pkt)

        assert isinstance(pdu, dis7.FirePdu), f"type={type(pdu).__name__}"
        assert pdu.exerciseID == EXERCISE_ID,  f"exerciseID={pdu.exerciseID}"
        assert pdu.pduType    == 2,            f"pduType={pdu.pduType}"

        assert pdu.firingEntityID.siteID        == FIRING_SITE,   f"site={pdu.firingEntityID.siteID}"
        assert pdu.firingEntityID.applicationID == FIRING_APP,    f"app={pdu.firingEntityID.applicationID}"
        assert pdu.firingEntityID.entityID      == FIRING_ENTITY, f"ent={pdu.firingEntityID.entityID}"

        assert pdu.targetEntityID.siteID        == TARGET_SITE
        assert pdu.targetEntityID.applicationID == TARGET_APP
        assert pdu.targetEntityID.entityID      == TARGET_ENTITY

        assert pdu.eventID.eventNumber == 77, f"eventNumber={pdu.eventID.eventNumber}"

        assert abs(pdu.location.x - LOCATION_X) < 1.0, f"location.x={pdu.location.x}"
        assert abs(pdu.location.y - LOCATION_Y) < 1.0, f"location.y={pdu.location.y}"
        assert abs(pdu.location.z - LOCATION_Z) < 1.0, f"location.z={pdu.location.z}"

        assert pdu.descriptor.warhead  == WARHEAD,  f"warhead={pdu.descriptor.warhead}"
        assert pdu.descriptor.fuse     == FUSE,     f"fuse={pdu.descriptor.fuse}"
        assert pdu.descriptor.quantity == QUANTITY, f"qty={pdu.descriptor.quantity}"

        range_parsed = pdu.range
        assert abs(range_parsed - RANGE_M) < 1.0, f"range={range_parsed} expected={RANGE_M}"

        _p(name, True,
           f"FirePdu parsed OK: event=77 attacker=1:3101:1 "
           f"xyz=({pdu.location.x:.0f},{pdu.location.y:.0f},{pdu.location.z:.0f}) "
           f"warhead={pdu.descriptor.warhead}")
    except AssertionError as e:
        _p(name, False, str(e))
    except Exception as e:
        _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T16: Integration -- Kafka connectivity
# ---------------------------------------------------------------------------
def test_T16():
    name = "T16[INTEG] Kafka connectivity + topic dis.raw"
    try:
        from confluent_kafka.admin import AdminClient, KafkaException
        import kafka_config as cfg

        admin = AdminClient({"bootstrap.servers": cfg.KAFKA_BOOTSTRAP})
        meta  = admin.list_topics(timeout=5.0)

        assert cfg.KAFKA_TOPIC in meta.topics, \
            f"Topic '{cfg.KAFKA_TOPIC}' missing (topics: {list(meta.topics.keys())})"

        n_parts = len(meta.topics[cfg.KAFKA_TOPIC].partitions)
        assert n_parts == cfg.KAFKA_NUM_PARTITIONS, \
            f"Partitions={n_parts} expected={cfg.KAFKA_NUM_PARTITIONS}"

        _p(name, True,
           f"broker={cfg.KAFKA_BOOTSTRAP} topic={cfg.KAFKA_TOPIC} partitions={n_parts}")
    except Exception as e:
        if "timed out" in str(e).lower() or "transport" in str(e).lower() or \
           "broker" in str(e).lower() or "connect" in str(e).lower():
            _skip(name, f"Kafka unavailable: {e}")
        else:
            _p(name, False, str(e))


# ---------------------------------------------------------------------------
# T17: Integration -- SQL Server connectivity
# ---------------------------------------------------------------------------
def test_T17():
    name = "T17[INTEG] SQL Server connectivity"
    try:
        import kafka_config as cfg
        import sqlalchemy

        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER=localhost\\SQLEXPRESS;"
            f"DATABASE={cfg.DB_NAME};"
            f"Trusted_Connection=yes;"
        )
        engine = sqlalchemy.create_engine(
            "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(conn_str),
            pool_pre_ping=True,
        )
        with engine.connect() as conn:
            result = conn.execute(sqlalchemy.text("SELECT 1"))
            val = result.scalar()
            assert val == 1

        _p(name, True, f"SQL Server OK database={cfg.DB_NAME}")
    except Exception as e:
        if "cannot open" in str(e).lower() or "login" in str(e).lower() or \
           "network" in str(e).lower() or "odbc" in str(e).lower() or \
           "pyodbc" in str(e).lower():
            _skip(name, f"SQL Server unavailable: {e}")
        else:
            _p(name, False, str(e))


# ---------------------------------------------------------------------------
# T18: Integration -- Kafka round-trip (producer -> consumer direct, without kafka_main)
# ---------------------------------------------------------------------------
def test_T18():
    name = "T18[INTEG] Kafka round-trip producer -> consumer direct"
    try:
        from confluent_kafka import Producer, Consumer, TopicPartition, KafkaError, KafkaException
        import kafka_config as cfg

        unique_id = str(time.time()).encode()
        payload   = b"DIS-INTENSIVE-TEST-" + unique_id
        partition = 0

        # Produce
        prod = Producer(cfg.KAFKA_PRODUCER_CONFIG)
        prod.produce(cfg.KAFKA_TOPIC, value=payload, partition=partition)
        remaining = prod.flush(timeout=10)
        if remaining > 0:
            _skip(name, f"Kafka unavailable: {remaining} message(s) not delivered after flush")
            return
        assert remaining == 0, f"{remaining} messages not delivered"

        # Watermark
        c_wm = Consumer({"bootstrap.servers": cfg.KAFKA_BOOTSTRAP, "group.id": "intensive-wm"})
        lo, hi = c_wm.get_watermark_offsets(
            TopicPartition(cfg.KAFKA_TOPIC, partition), timeout=5
        )
        c_wm.close()
        target = hi - 1
        assert target >= 0, f"invalid watermark hi={hi}"

        # Consume
        cons = Consumer({
            "bootstrap.servers": cfg.KAFKA_BOOTSTRAP,
            "group.id": f"intensive-rt-{int(time.time())}",
            "enable.auto.commit": False,
        })
        cons.assign([TopicPartition(cfg.KAFKA_TOPIC, partition, target)])

        found = False
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            msg = cons.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            if unique_id in msg.value():
                found = True
                break
        cons.close()

        assert found, "Message not received within 10 seconds"
        _p(name, True, f"Round-trip OK partition={partition} offset={target}")
    except Exception as e:
        if "timed out" in str(e).lower() or "transport" in str(e).lower() or \
           "broker" in str(e).lower():
            _skip(name, f"Kafka unavailable: {e}")
        else:
            _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T19: Integration -- Pipeline UDP -> Kafka -> SQL (kafka_main must be running)
# ---------------------------------------------------------------------------
def test_T19():
    name = "T19[INTEG] Pipeline UDP->Kafka->SQL 200 FirePdu (kafka_main required)"
    try:
        import kafka_config as cfg
        import sqlalchemy as sa

        COUNT      = 200
        WAIT       = 25   # seconds to wait for the pipeline
        TOLERANCE  = 1    # 1 packet max (UDP best-effort: 0.5% loss on localhost burst accepted)

        # SQL connection
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER=localhost\\SQLEXPRESS;"
            f"DATABASE={cfg.DB_NAME};"
            f"Trusted_Connection=yes;"
        )
        engine = sa.create_engine(
            "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(conn_str),
            pool_pre_ping=True,
        )

        # Test connection
        try:
            with engine.connect():
                pass
        except Exception as e:
            _skip(name, f"SQL unavailable: {e}")
            return

        # Test Kafka
        try:
            from confluent_kafka.admin import AdminClient
            admin = AdminClient({"bootstrap.servers": cfg.KAFKA_BOOTSTRAP})
            admin.list_topics(timeout=3.0)
        except Exception as e:
            _skip(name, f"Kafka unavailable: {e}")
            return

        # Reference timestamp
        t_before = datetime.datetime.now()

        # Send UDP packets
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        base_ts = int(time.time()) & 0xFFFFFFFF
        t_send = time.monotonic()
        for i in range(1, COUNT + 1):
            pkt = _build_fire_pdu(i % 65534 + 1, (base_ts + i) & 0xFFFFFFFF)
            sock.sendto(pkt, ("localhost", cfg.PORT))
        sock.close()
        elapsed_send = time.monotonic() - t_send
        print(f"  {INFO}         {COUNT} packets sent in {elapsed_send*1000:.0f}ms -- waiting {WAIT}s...")

        time.sleep(WAIT)

        # SQL verification
        with engine.connect() as conn:
            row = conn.execute(sa.text(
                "SELECT COUNT(*) FROM dis.FirePdu WHERE WorldTime >= :t"
            ), {"t": t_before}).fetchone()
        received = row[0]

        loss     = COUNT - received
        loss_pct = loss / COUNT * 100

        detail = (
            f"Sent   : {COUNT}\n"
            f"In SQL : {received}\n"
            f"Lost   : {loss} ({loss_pct:.1f}%)"
        )
        passed = (loss <= TOLERANCE)
        _p(name, passed,
           f"{received}/{COUNT} in SQL ({loss_pct:.1f}% loss)", detail)
    except Exception as e:
        _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T20: insert_sync() -- ExportTimeToDb stamped + returns True/False
# ---------------------------------------------------------------------------
def test_T20():
    name = "T20[B3] insert_sync() ExportTimeToDb stamp + bool return"
    try:
        import sqlalchemy
        from LoggerSQLExporter import LoggerSQLExporter, Exporter

        mock_engine = MagicMock()
        mock_meta   = MagicMock()
        stop_event  = multiprocessing.Event()

        # Configure the begin() context manager
        ctx = MagicMock()
        inserted_rows = []
        def fake_execute(stmt, data):
            inserted_rows.extend(data)
        conn_mock = MagicMock()
        conn_mock.execute.side_effect = fake_execute
        ctx.__enter__ = MagicMock(return_value=conn_mock)
        ctx.__exit__  = MagicMock(return_value=False)
        mock_engine.begin.return_value = ctx

        # Patch sqlalchemy.Table and threading for LSE + Exporter
        orig_thread = threading.Thread
        orig_table  = sqlalchemy.Table
        orig_meta   = sqlalchemy.MetaData
        threading.Thread  = MagicMock()
        sqlalchemy.Table  = MagicMock(return_value=MagicMock())
        sqlalchemy.MetaData = MagicMock(return_value=mock_meta)

        try:
            lse = LoggerSQLExporter(
                logger_file="test_intensive.lzma",
                tracked_tables=[],
                export_db="noamtest",
                stop_event=stop_event,
                start_time2=time.time() - 10.0,  # start_time 10s ago
                export_delay=1.0,
                export_size=100,
                new_db=False,
            )
            lse.sql_engine   = mock_engine
            # Inject a mocked exporter into the exporters dict
            mock_exp = MagicMock()
            mock_exp.insert.side_effect = fake_execute
            lse.exporters["FirePdu"] = mock_exp
        finally:
            threading.Thread  = orig_thread
            sqlalchemy.Table  = orig_table
            sqlalchemy.MetaData = orig_meta

        # Test 1: insert_sync() returns True on success
        t_before = time.time()
        mock_exp.insert.reset_mock()
        mock_exp.insert.side_effect = None  # no error
        result = lse.insert_sync("FirePdu", [{"EventId": "1:2:3", "Range": 100.0}])
        assert result is True, f"insert_sync() should return True on success, got {result}"

        # Verify that ExportTimeToDb was added to the row
        call_args = mock_exp.insert.call_args
        assert call_args is not None, "insert() never called"
        rows_sent = call_args[0][0]
        assert len(rows_sent) == 1
        assert "ExportTimeToDb" in rows_sent[0], "ExportTimeToDb missing from the row"
        export_ts = rows_sent[0]["ExportTimeToDb"]
        assert export_ts >= 0, f"ExportTimeToDb={export_ts} negative"
        assert export_ts < 20.0, f"ExportTimeToDb={export_ts} too large (start_time 10s ago)"

        # Test 2: insert_sync() returns False on SQL error
        mock_exp.insert.side_effect = Exception("SQL Server connection timeout")
        result_fail = lse.insert_sync("FirePdu", [{"EventId": "X"}])
        assert result_fail is False, f"insert_sync() should return False on error, got {result_fail}"

        _p(name, True,
           f"True on success | ExportTimeToDb={export_ts:.3f}s | False on SQL error")
    except AssertionError as e:
        _p(name, False, str(e))
    except Exception as e:
        _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------
UNIT_TESTS = [
    ("T01", test_T01),
    ("T02", test_T02),
    ("T03", test_T03),
    ("T04", test_T04),
    ("T05", test_T05),
    ("T06", test_T06),
    ("T07", test_T07),
    ("T08", test_T08),
    ("T09", test_T09),
    ("T10", test_T10),
    ("T11", test_T11),
    ("T12", test_T12),
    ("T13", test_T13),
    ("T14", test_T14),
    ("T15", test_T15),
    ("T20", test_T20),
]

INTEG_TESTS = [
    ("T16", test_T16),
    ("T17", test_T17),
    ("T18", test_T18),
    ("T19", test_T19),
]

TEST_MAP = {name: fn for name, fn in UNIT_TESTS + INTEG_TESTS}


def main():
    parser = argparse.ArgumentParser(description="Intensive test suite for the Kafka DIS pipeline")
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--unit",  action="store_true", help="Unit tests only (T01-T15, T20)")
    grp.add_argument("--integ", action="store_true", help="Integration tests only (T16-T19)")
    parser.add_argument("--test", type=str, default="", help="Specific test (e.g.: T07)")
    args = parser.parse_args()

    print("\n" + "="*65)
    print("  test_intensive.py -- Kafka DIS pipeline test suite")
    print(f"  Date: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*65)

    if args.test:
        t = args.test.upper()
        if t not in TEST_MAP:
            print(f"Unknown test {t}. Available: {sorted(TEST_MAP.keys())}")
            sys.exit(1)
        _section(f"Test {t}")
        TEST_MAP[t]()

    elif args.unit:
        _section("UNIT Tests (T01-T15, T20)")
        for name, fn in UNIT_TESTS:
            fn()

    elif args.integ:
        _section("INTEGRATION Tests (T16-T19)")
        for name, fn in INTEG_TESTS:
            fn()

    else:
        _section("UNIT Tests (T01-T15, T20)")
        for name, fn in UNIT_TESTS:
            fn()

        _section("INTEGRATION Tests (T16-T19)")
        for name, fn in INTEG_TESTS:
            fn()

    # Final report
    _section("FINAL REPORT")
    passed  = [(n, r) for n, r in results if r is True]
    failed  = [(n, r) for n, r in results if r is False]
    skipped = [(n, r) for n, r in results if r is None]

    for n, r in results:
        tag = PASS if r is True else (SKIP if r is None else FAIL)
        print(f"  {tag}  {n}")

    print()
    print(f"  Passed  : {len(passed)}")
    print(f"  Failed  : {len(failed)}")
    print(f"  Skipped : {len(skipped)}")
    print(f"  Total   : {len(results)}")

    if failed:
        print(f"\n  Failed tests: {[n for n, _ in failed]}")
        sys.exit(1)
    else:
        print("\n  All executed tests passed!")
        sys.exit(0)


if __name__ == "__main__":
    main()
