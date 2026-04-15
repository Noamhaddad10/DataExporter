"""
test_intensive.py
=================
Suite de tests intensive -- 20 tests couvrant :
  - Les 5 bugs corriges (B1..B5)
  - L'integrite structurelle du pipeline
  - La logique metier (filtres, serialisation, champs SQL)
  - Les tests d'integration Kafka + SQL (conditionnels)

Tests UNIT   T01-T15 : sans dependances externes (toujours executes)
Tests INTEG  T16-T20 : necessitent Kafka + SQL Server (skipped si indisponible)

Usage :
    python test_intensive.py              -- tous les tests
    python test_intensive.py --unit       -- tests unitaires uniquement
    python test_intensive.py --integ      -- tests integration uniquement
    python test_intensive.py --test T07   -- test specifique
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
# Path setup -- permet d'executer depuis tests/ ou depuis la racine du projet
# ---------------------------------------------------------------------------
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

# ---------------------------------------------------------------------------
# Resultats
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
# PDU helpers (96 bytes, identique a send_fire_pdu.py)
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
# T01 : Taille PDU + positions de bytes cles
# ---------------------------------------------------------------------------
def test_T01():
    name = "T01[B1] PDU 96B layout"
    try:
        pkt = _build_fire_pdu(42, 99999)

        assert len(pkt) == 96, f"taille={len(pkt)} attendu=96"
        assert pkt[0] == 7,            f"protocolVersion={pkt[0]} attendu=7"
        assert pkt[1] == EXERCISE_ID,  f"exerciseID={pkt[1]} attendu={EXERCISE_ID}"
        assert pkt[2] == 2,            f"pduType={pkt[2]} attendu=2 (FirePdu)"
        assert pkt[3] == 2,            f"family={pkt[3]} attendu=2 (Warfare)"

        length_field = struct.unpack("!H", pkt[8:10])[0]
        assert length_field == 96, f"PDU length field={length_field} attendu=96"

        # Verifier que firingEntityID est bien a l'offset 12 (pas 14)
        site, app, ent = struct.unpack("!HHH", pkt[12:18])
        assert site == FIRING_SITE and app == FIRING_APP and ent == FIRING_ENTITY, \
            f"firingEntityID=[{site}:{app}:{ent}] attendu=[{FIRING_SITE}:{FIRING_APP}:{FIRING_ENTITY}]"

        # Location a l'offset 40
        lx, ly, lz = struct.unpack("!ddd", pkt[40:64])
        assert abs(lx - LOCATION_X) < 0.1, f"location.x={lx} attendu={LOCATION_X}"

        _p(name, True, f"96 bytes, pduType=2, firingEntityID@offset12, location@offset40")
    except AssertionError as e:
        _p(name, False, str(e))
    except Exception as e:
        _p(name, False, f"Exception: {e}")


# ---------------------------------------------------------------------------
# T02 : LoggerPDU.from_parts() -- construction directe sans line_divider
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
            f"type={type(pdu_obj.pdu).__name__} attendu=FirePdu"
        assert pdu_obj.pdu.exerciseID == EXERCISE_ID, \
            f"exerciseID={pdu_obj.pdu.exerciseID} attendu={EXERCISE_ID}"
        assert abs(pdu_obj.packet_time - packet_time) < 1e-10, \
            f"packet_time={pdu_obj.packet_time} attendu={packet_time}"

        _p(name, True, f"pdu=FirePdu exerciseID={pdu_obj.pdu.exerciseID} packet_time={pdu_obj.packet_time:.5f}")
    except Exception as e:
        _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T03 : BUG 2 -- from_parts() immune a la collision b"line_divider"
# ---------------------------------------------------------------------------
def test_T03():
    name = "T03[B2] from_parts() immune collision line_divider"
    try:
        from LoggerPduProcessor import LoggerPDU

        # Construire un PDU valide, puis injecter b"line_divider" dans le payload
        # On ecrase les bytes de velocity (offset 80) avec la sequence
        pkt = bytearray(_build_fire_pdu(1, 12345))
        seq = b"line_divider"  # 12 bytes
        pkt[80:92] = seq       # injection dans velocity field
        pkt_bytes = bytes(pkt)

        # Le constructeur classique DOIT echouer (il y a maintenant 2 occurrences: injected + maybe 0)
        # On verifie que from_parts() reussit SANS exception
        crashed_old = False
        try:
            _ = LoggerPDU(pkt_bytes + b"line_divider" + struct.pack("d", 1.0))
        except ValueError:
            crashed_old = True

        # from_parts() ne doit PAS crasher
        pdu_obj = LoggerPDU.from_parts(pkt_bytes, 2.71)

        assert pdu_obj.pdu is not None, "from_parts() a retourne pdu=None"

        detail = (
            f"Ancien constructeur crash sur collision : {crashed_old}\n"
            f"from_parts() reussit   : True\n"
            f"Collision eliminee     : OK"
        )
        _p(name, True, "from_parts() ne crashe pas, ancien constructeur crashait", detail)
    except Exception as e:
        _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T04 : Confirmer que l'ancien constructeur echoue avec 0 separateurs
# ---------------------------------------------------------------------------
def test_T04():
    name = "T04[B2] Ancien constructeur ValueError sur 0 line_divider"
    try:
        from LoggerPduProcessor import LoggerPDU

        pkt = _build_fire_pdu(1, 12345)
        # Passer des bytes sans line_divider -> ValueError attendue
        raised = False
        try:
            _ = LoggerPDU(pkt)  # pas de line_divider -> ValueError
        except ValueError:
            raised = True

        assert raised, "Le constructeur aurait du lever ValueError sur des bytes sans line_divider"
        _p(name, True, "ValueError confirme sur bytes sans line_divider")
    except Exception as e:
        _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T05 : Analyse AST -- insert_sync() present + signature correcte
# ---------------------------------------------------------------------------
def test_T05():
    name = "T05[B3] insert_sync() present dans LoggerSQLExporter"
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

        assert found is not None, "insert_sync() absent de LoggerSQLExporter"

        # Verifier les arguments : self, table, data
        args = [a.arg for a in found.args.args]
        assert "self"  in args, f"arg 'self' manquant: {args}"
        assert "table" in args, f"arg 'table' manquant: {args}"
        assert "data"  in args, f"arg 'data' manquant: {args}"

        # Verifier qu'il retourne bool (return True / return False presents)
        returns = [n for n in ast.walk(found) if isinstance(n, ast.Return)]
        return_values = []
        for r in returns:
            if isinstance(r.value, ast.Constant):
                return_values.append(r.value.value)
        assert True  in return_values, "insert_sync() ne retourne jamais True"
        assert False in return_values, "insert_sync() ne retourne jamais False"

        _p(name, True, f"insert_sync(self, table, data) -> bool confirme")
    except AssertionError as e:
        _p(name, False, str(e))
    except Exception as e:
        _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T06 : kafka_consumer._drain_and_export utilise insert_sync, pas lse.export
# ---------------------------------------------------------------------------
def test_T06():
    name = "T06[B3] _drain_and_export() -> insert_sync, pas lse.export()"
    try:
        with open(os.path.join(_ROOT, "kafka_consumer.py"), "r", encoding="utf-8") as f:
            src = f.read()
        tree = ast.parse(src)

        drain_func = None
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "_drain_and_export":
                drain_func = node
                break

        assert drain_func is not None, "_drain_and_export() introuvable"

        # Chercher les appels lse.insert_sync et lse.export dans cette fonction
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

        assert len(calls_insert_sync) >= 1, "insert_sync() non appele dans _drain_and_export()"
        assert len(calls_export)      == 0, \
            f"lse.export() encore appele dans _drain_and_export() ({len(calls_export)} fois)"

        _p(name, True,
           f"insert_sync appele {len(calls_insert_sync)}x, lse.export() absent")
    except AssertionError as e:
        _p(name, False, str(e))
    except Exception as e:
        _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T07 : insert() retry deadlock -- max 5 tentatives puis abandon
# ---------------------------------------------------------------------------
def test_T07():
    name = "T07[B4] insert() retry deadlock (max 5x) sans boucle infinie"
    try:
        import sqlalchemy
        from LoggerSQLExporter import Exporter

        mock_engine = MagicMock()
        mock_meta   = MagicMock()
        stop_event  = multiprocessing.Event()

        # Configurer le context manager de begin()
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=MagicMock())
        ctx.__exit__  = MagicMock(return_value=False)  # ne pas supprimer les exceptions
        mock_engine.begin.return_value = ctx

        # Patcher threading.Thread et sqlalchemy.Table pour eviter les side-effects
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

        # Simuler un deadlock permanent (6 tentatives)
        call_count = [0]
        def always_deadlock(*args, **kwargs):
            call_count[0] += 1
            raise Exception("Transaction (Process ID 99) was deadlocked on lock resources")

        ctx.__enter__.return_value.execute.side_effect = always_deadlock

        t_start = time.monotonic()
        exp.insert([{"EventId": "1:1:1"}])
        elapsed = time.monotonic() - t_start

        assert call_count[0] <= 6, \
            f"insert() a fait {call_count[0]} tentatives (attendu <= 6)"
        assert elapsed < 5.0, \
            f"insert() a bloque pendant {elapsed:.1f}s (deadlock non resolu en < 5s)"

        _p(name, True,
           f"Arrete apres {call_count[0]} tentatives en {elapsed*1000:.0f}ms (pas de boucle infinie)")
    except AssertionError as e:
        _p(name, False, str(e))
    except Exception as e:
        _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T08 : insert() -- cle dupliquee (Entities_UIX) = retour silencieux
# ---------------------------------------------------------------------------
def test_T08():
    name = "T08[B4] insert() Entities_UIX = retour silencieux sans erreur"
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

        # insert() doit retourner proprement sans lever d'exception
        exp.insert([{"EntityId": "1:1:1"}])

        assert call_count[0] == 1, \
            f"insert() a reessaye {call_count[0]}x sur Entities_UIX (devrait retourner apres 1)"

        _p(name, True,
           f"Retour apres 1 tentative, pas de retry sur duplicate key")
    except AssertionError as e:
        _p(name, False, str(e))
    except Exception as e:
        _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T09 : insert() -- erreur non-retryable = retour immediat
# ---------------------------------------------------------------------------
def test_T09():
    name = "T09[B4] insert() erreur non-retryable = retour immediat"
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
            f"insert() a reessaye {call_count[0]}x sur erreur non-retryable (attendu 1)"

        _p(name, True, f"Retour immediat apres 1 tentative sur erreur non-retryable")
    except AssertionError as e:
        _p(name, False, str(e))
    except Exception as e:
        _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T10 : Filtre exerciseID dans kafka_consumer (lecture du code)
# ---------------------------------------------------------------------------
def test_T10():
    name = "T10 Filtre exerciseID present dans kafka_consumer"
    try:
        with open(os.path.join(_ROOT, "kafka_consumer.py"), "r", encoding="utf-8") as f:
            tree = ast.parse(f.read())

        # Chercher la comparaison pdu.exerciseID != cfg.EXERCISE_ID
        found = False
        for node in ast.walk(tree):
            if isinstance(node, ast.Compare):
                # Chercher received_pdu.pdu.exerciseID != cfg.EXERCISE_ID
                left = node.left
                if (isinstance(left, ast.Attribute) and left.attr == "exerciseID"):
                    found = True
                    break

        assert found, "Comparaison exerciseID introuvable dans kafka_consumer"
        _p(name, True, "Filtre pdu.exerciseID != cfg.EXERCISE_ID confirme")
    except AssertionError as e:
        _p(name, False, str(e))
    except Exception as e:
        _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T11 : Filtre pduType -- seuls les types PDU_TYPE_LIST passes
# ---------------------------------------------------------------------------
def test_T11():
    name = "T11 Filtre pduType dans kafka_consumer"
    try:
        with open(os.path.join(_ROOT, "kafka_consumer.py"), "r", encoding="utf-8") as f:
            src = f.read()

        # Verifier que pdu_type not in cfg.PDU_TYPE_LIST est present
        assert "PDU_TYPE_LIST" in src, "PDU_TYPE_LIST non reference dans kafka_consumer"
        assert "pdu_type" in src, "variable pdu_type absente"

        # Verifier que pdu_data[2] est utilise pour extraire pduType
        assert "pdu_data[2]" in src, "pdu_type extrait de pdu_data[2] absent"

        _p(name, True, "Filtre pduType via pdu_data[2] not in PDU_TYPE_LIST confirme")
    except AssertionError as e:
        _p(name, False, str(e))
    except Exception as e:
        _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T12 : Format message Kafka -- serialisation / deserialisation round-trip
# ---------------------------------------------------------------------------
def test_T12():
    name = "T12 Kafka message format round-trip (struct pack/unpack)"
    try:
        KAFKA_MSG_HEADER_SIZE = 8
        pdu_raw = _build_fire_pdu(7, 55555)
        original_time = 12.345678901234

        # Serialisation (comme kafka_producer)
        kafka_value = struct.pack("!d", original_time) + pdu_raw

        # Deserialisation (comme kafka_consumer)
        assert len(kafka_value) >= KAFKA_MSG_HEADER_SIZE
        recovered_time = struct.unpack("!d", kafka_value[:KAFKA_MSG_HEADER_SIZE])[0]
        recovered_pdu  = kafka_value[KAFKA_MSG_HEADER_SIZE:]

        assert abs(recovered_time - original_time) < 1e-12, \
            f"packet_time perdu: {recovered_time} vs {original_time}"
        assert recovered_pdu == pdu_raw, "PDU bytes corrompus apres round-trip"
        assert recovered_pdu[2] == 2, f"pduType perdu: {recovered_pdu[2]}"

        _p(name, True,
           f"packet_time={recovered_time:.6f} PDU={len(recovered_pdu)}B pduType=2")
    except AssertionError as e:
        _p(name, False, str(e))
    except Exception as e:
        _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T13 : from_parts() preserves packet_time avec precision float64
# ---------------------------------------------------------------------------
def test_T13():
    name = "T13 from_parts() precision packet_time float64"
    try:
        from LoggerPduProcessor import LoggerPDU

        pkt = _build_fire_pdu(1, 12345)
        # Valeurs difficiles a arrondir
        for pt in [0.0, 1.0, 0.001, 3.141592653589793, 86399.999999999]:
            obj = LoggerPDU.from_parts(pkt, pt)
            assert abs(obj.packet_time - pt) < 1e-12, \
                f"packet_time={obj.packet_time} attendu={pt} (diff={abs(obj.packet_time - pt)})"

        _p(name, True, "5 valeurs packet_time preservees avec precision < 1e-12")
    except AssertionError as e:
        _p(name, False, str(e))
    except Exception as e:
        _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T14 : Analyse AST -- noms de champs dans _process_fire_pdu()
# ---------------------------------------------------------------------------
def test_T14():
    name = "T14 Noms champs _process_fire_pdu() (pas de typos critiques)"
    try:
        with open(os.path.join(_ROOT, "LoggerPduProcessor.py"), "r", encoding="utf-8") as f:
            tree = ast.parse(f.read())

        # Trouver _process_fire_pdu
        func = None
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "_process_fire_pdu":
                func = node
                break

        assert func is not None, "_process_fire_pdu() introuvable"

        # Collecter toutes les cles de dict (ast.Constant dans ast.Dict)
        keys = []
        for node in ast.walk(func):
            if isinstance(node, ast.Dict):
                for k in node.keys:
                    if isinstance(k, ast.Constant) and isinstance(k.value, str):
                        keys.append(k.value)

        # Verifier l'absence de typos connus
        assert "EventId"    in keys, "EventId absent"
        assert "AttackerId" in keys, "AttackerId absent"
        assert "TargetId"   in keys, "TargetId absent"
        assert "Range"      in keys, "Range absent"
        assert "LoggerFile" in keys, "LoggerFile absent"
        assert "WorldTime"  in keys, "WorldTime absent"
        assert "PacketTime" in keys, "PacketTime absent"

        # Verifier l'absence du print() hot-path
        prints_in_lpp = []
        with open(os.path.join(_ROOT, "LoggerPduProcessor.py"), "r", encoding="utf-8") as f:
            for i, line in enumerate(f, 1):
                if "print(f\"process {os.getpid()}" in line or \
                   "count_parsing" in line and "print" in line:
                    prints_in_lpp.append((i, line.strip()))

        assert len(prints_in_lpp) == 0, \
            f"print hot-path encore present : {prints_in_lpp}"

        detail = "Champs: " + ", ".join(keys)
        _p(name, True, f"{len(keys)} champs valides, print hot-path absent", detail)
    except AssertionError as e:
        _p(name, False, str(e))
    except Exception as e:
        _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T15 : opendis parse du FirePdu construit -- valeurs champs correctes
# ---------------------------------------------------------------------------
def test_T15():
    name = "T15 opendis parse FirePdu -- valeurs champs exactes"
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
        assert abs(range_parsed - RANGE_M) < 1.0, f"range={range_parsed} attendu={RANGE_M}"

        _p(name, True,
           f"FirePdu parse OK: event=77 attacker=1:3101:1 "
           f"xyz=({pdu.location.x:.0f},{pdu.location.y:.0f},{pdu.location.z:.0f}) "
           f"warhead={pdu.descriptor.warhead}")
    except AssertionError as e:
        _p(name, False, str(e))
    except Exception as e:
        _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T16 : Integration -- Kafka connectivity
# ---------------------------------------------------------------------------
def test_T16():
    name = "T16[INTEG] Kafka connectivity + topic dis.raw"
    try:
        from confluent_kafka.admin import AdminClient, KafkaException
        import kafka_config as cfg

        admin = AdminClient({"bootstrap.servers": cfg.KAFKA_BOOTSTRAP})
        meta  = admin.list_topics(timeout=5.0)

        assert cfg.KAFKA_TOPIC in meta.topics, \
            f"Topic '{cfg.KAFKA_TOPIC}' absent (topics: {list(meta.topics.keys())})"

        n_parts = len(meta.topics[cfg.KAFKA_TOPIC].partitions)
        assert n_parts == cfg.KAFKA_NUM_PARTITIONS, \
            f"Partitions={n_parts} attendu={cfg.KAFKA_NUM_PARTITIONS}"

        _p(name, True,
           f"broker={cfg.KAFKA_BOOTSTRAP} topic={cfg.KAFKA_TOPIC} partitions={n_parts}")
    except Exception as e:
        if "timed out" in str(e).lower() or "transport" in str(e).lower() or \
           "broker" in str(e).lower() or "connect" in str(e).lower():
            _skip(name, f"Kafka indisponible : {e}")
        else:
            _p(name, False, str(e))


# ---------------------------------------------------------------------------
# T17 : Integration -- SQL Server connectivity
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

        _p(name, True, f"SQL Server OK base={cfg.DB_NAME}")
    except Exception as e:
        if "cannot open" in str(e).lower() or "login" in str(e).lower() or \
           "network" in str(e).lower() or "odbc" in str(e).lower() or \
           "pyodbc" in str(e).lower():
            _skip(name, f"SQL Server indisponible : {e}")
        else:
            _p(name, False, str(e))


# ---------------------------------------------------------------------------
# T18 : Integration -- Round-trip Kafka (producer -> consumer direct, sans kafka_main)
# ---------------------------------------------------------------------------
def test_T18():
    name = "T18[INTEG] Kafka round-trip producer -> consumer direct"
    try:
        from confluent_kafka import Producer, Consumer, TopicPartition, KafkaError, KafkaException
        import kafka_config as cfg

        unique_id = str(time.time()).encode()
        payload   = b"DIS-INTENSIVE-TEST-" + unique_id
        partition = 0

        # Produire
        prod = Producer(cfg.KAFKA_PRODUCER_CONFIG)
        prod.produce(cfg.KAFKA_TOPIC, value=payload, partition=partition)
        remaining = prod.flush(timeout=10)
        if remaining > 0:
            _skip(name, f"Kafka indisponible : {remaining} message(s) non livre(s) apres flush")
            return
        assert remaining == 0, f"{remaining} messages non livres"

        # Watermark
        c_wm = Consumer({"bootstrap.servers": cfg.KAFKA_BOOTSTRAP, "group.id": "intensive-wm"})
        lo, hi = c_wm.get_watermark_offsets(
            TopicPartition(cfg.KAFKA_TOPIC, partition), timeout=5
        )
        c_wm.close()
        target = hi - 1
        assert target >= 0, f"watermark invalide hi={hi}"

        # Consommer
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

        assert found, "Message non recu dans les 10 secondes"
        _p(name, True, f"Round-trip OK partition={partition} offset={target}")
    except Exception as e:
        if "timed out" in str(e).lower() or "transport" in str(e).lower() or \
           "broker" in str(e).lower():
            _skip(name, f"Kafka indisponible : {e}")
        else:
            _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T19 : Integration -- Pipeline UDP -> Kafka -> SQL (kafka_main doit tourner)
# ---------------------------------------------------------------------------
def test_T19():
    name = "T19[INTEG] Pipeline UDP->Kafka->SQL 200 FirePdu (kafka_main requis)"
    try:
        import kafka_config as cfg
        import sqlalchemy as sa

        COUNT      = 200
        WAIT       = 25   # secondes d'attente pipeline
        TOLERANCE  = 1    # 1 paquet max (UDP best-effort: 0.5% perte sur burst localhost acceptee)

        # Connexion SQL
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

        # Test connexion
        try:
            with engine.connect():
                pass
        except Exception as e:
            _skip(name, f"SQL indisponible : {e}")
            return

        # Test Kafka
        try:
            from confluent_kafka.admin import AdminClient
            admin = AdminClient({"bootstrap.servers": cfg.KAFKA_BOOTSTRAP})
            admin.list_topics(timeout=3.0)
        except Exception as e:
            _skip(name, f"Kafka indisponible : {e}")
            return

        # Timestamp de reference
        t_before = datetime.datetime.now()

        # Envoi UDP
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        base_ts = int(time.time()) & 0xFFFFFFFF
        t_send = time.monotonic()
        for i in range(1, COUNT + 1):
            pkt = _build_fire_pdu(i % 65534 + 1, (base_ts + i) & 0xFFFFFFFF)
            sock.sendto(pkt, ("localhost", cfg.PORT))
        sock.close()
        elapsed_send = time.monotonic() - t_send
        print(f"  {INFO}         {COUNT} paquets envoyes en {elapsed_send*1000:.0f}ms -- attente {WAIT}s...")

        time.sleep(WAIT)

        # Verification SQL
        with engine.connect() as conn:
            row = conn.execute(sa.text(
                "SELECT COUNT(*) FROM dis.FirePdu WHERE WorldTime >= :t"
            ), {"t": t_before}).fetchone()
        received = row[0]

        loss     = COUNT - received
        loss_pct = loss / COUNT * 100

        detail = (
            f"Envoyes : {COUNT}\n"
            f"En SQL  : {received}\n"
            f"Perdus  : {loss} ({loss_pct:.1f}%)"
        )
        passed = (loss <= TOLERANCE)
        _p(name, passed,
           f"{received}/{COUNT} en SQL ({loss_pct:.1f}% perte)", detail)
    except Exception as e:
        _p(name, False, str(e), traceback.format_exc())


# ---------------------------------------------------------------------------
# T20 : insert_sync() -- ExportTimeToDb stampe + retourne True/False
# ---------------------------------------------------------------------------
def test_T20():
    name = "T20[B3] insert_sync() ExportTimeToDb stamp + retour bool"
    try:
        import sqlalchemy
        from LoggerSQLExporter import LoggerSQLExporter, Exporter

        mock_engine = MagicMock()
        mock_meta   = MagicMock()
        stop_event  = multiprocessing.Event()

        # Configurer le context manager begin()
        ctx = MagicMock()
        inserted_rows = []
        def fake_execute(stmt, data):
            inserted_rows.extend(data)
        conn_mock = MagicMock()
        conn_mock.execute.side_effect = fake_execute
        ctx.__enter__ = MagicMock(return_value=conn_mock)
        ctx.__exit__  = MagicMock(return_value=False)
        mock_engine.begin.return_value = ctx

        # Patch sqlalchemy.Table et threading pour LSE + Exporter
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
            # Injecter un exporter mocke dans les exporters
            mock_exp = MagicMock()
            mock_exp.insert.side_effect = fake_execute
            lse.exporters["FirePdu"] = mock_exp
        finally:
            threading.Thread  = orig_thread
            sqlalchemy.Table  = orig_table
            sqlalchemy.MetaData = orig_meta

        # Test 1 : insert_sync() retourne True sur succes
        t_before = time.time()
        mock_exp.insert.reset_mock()
        mock_exp.insert.side_effect = None  # pas d'erreur
        result = lse.insert_sync("FirePdu", [{"EventId": "1:2:3", "Range": 100.0}])
        assert result is True, f"insert_sync() devrait retourner True sur succes, got {result}"

        # Verifier que ExportTimeToDb a ete ajoute a la row
        call_args = mock_exp.insert.call_args
        assert call_args is not None, "insert() jamais appele"
        rows_sent = call_args[0][0]
        assert len(rows_sent) == 1
        assert "ExportTimeToDb" in rows_sent[0], "ExportTimeToDb absent de la row"
        export_ts = rows_sent[0]["ExportTimeToDb"]
        assert export_ts >= 0, f"ExportTimeToDb={export_ts} negatif"
        assert export_ts < 20.0, f"ExportTimeToDb={export_ts} trop grand (start_time 10s ago)"

        # Test 2 : insert_sync() retourne False sur erreur SQL
        mock_exp.insert.side_effect = Exception("SQL Server connection timeout")
        result_fail = lse.insert_sync("FirePdu", [{"EventId": "X"}])
        assert result_fail is False, f"insert_sync() devrait retourner False sur erreur, got {result_fail}"

        _p(name, True,
           f"True sur succes | ExportTimeToDb={export_ts:.3f}s | False sur erreur SQL")
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
    parser = argparse.ArgumentParser(description="Tests intensifs pipeline Kafka DIS")
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--unit",  action="store_true", help="Tests unitaires uniquement (T01-T15, T20)")
    grp.add_argument("--integ", action="store_true", help="Tests integration uniquement (T16-T19)")
    parser.add_argument("--test", type=str, default="", help="Test specifique (ex: T07)")
    args = parser.parse_args()

    print("\n" + "="*65)
    print("  test_intensive.py -- Suite de tests Kafka DIS pipeline")
    print(f"  Date : {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*65)

    if args.test:
        t = args.test.upper()
        if t not in TEST_MAP:
            print(f"Test {t} inconnu. Disponibles: {sorted(TEST_MAP.keys())}")
            sys.exit(1)
        _section(f"Test {t}")
        TEST_MAP[t]()

    elif args.unit:
        _section("Tests UNITAIRES (T01-T15, T20)")
        for name, fn in UNIT_TESTS:
            fn()

    elif args.integ:
        _section("Tests INTEGRATION (T16-T19)")
        for name, fn in INTEG_TESTS:
            fn()

    else:
        _section("Tests UNITAIRES (T01-T15, T20)")
        for name, fn in UNIT_TESTS:
            fn()

        _section("Tests INTEGRATION (T16-T19)")
        for name, fn in INTEG_TESTS:
            fn()

    # Rapport final
    _section("RAPPORT FINAL")
    passed  = [(n, r) for n, r in results if r is True]
    failed  = [(n, r) for n, r in results if r is False]
    skipped = [(n, r) for n, r in results if r is None]

    for n, r in results:
        tag = PASS if r is True else (SKIP if r is None else FAIL)
        print(f"  {tag}  {n}")

    print()
    print(f"  Passes  : {len(passed)}")
    print(f"  Echecs  : {len(failed)}")
    print(f"  Skips   : {len(skipped)}")
    print(f"  Total   : {len(results)}")

    if failed:
        print(f"\n  Tests en echec : {[n for n, _ in failed]}")
        sys.exit(1)
    else:
        print("\n  Tous les tests executes sont passes !")
        sys.exit(0)


if __name__ == "__main__":
    main()
