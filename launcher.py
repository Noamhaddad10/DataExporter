"""
launcher.py
===========
Data Exporter Launcher -- graphical UI to start the DIS -> Kafka -> SQL pipeline.

Provides:
- Light / Dark theme toggle (persisted)
- DEV / PROD mode toggle (single panel that swaps content)
- "START EVERYTHING": writes the active preset to DataExporterConfig.json,
  spawns Kafka broker + kafka_main.py, waits for both ready
- "STOP EVERYTHING": graceful CTRL_BREAK_EVENT -> kill fallback
- IDLE / RUNNING views with full-screen real-time log when running
- Live tail of dis-kafka.log
- Periodic status checks (Kafka port + SQL connectivity)
- Persistent presets in LauncherPresets.json
- Advanced config opens DataExporterConfig.json in Notepad

Build support: detects sys.frozen to spawn the .exe versions of Kafka/kafka_main
when running from a PyInstaller bundle. In source mode uses .venv\\Scripts\\python.exe.
"""

import json
import os
import signal
import socket
import struct
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path

from PyQt5.QtCore import Qt, QThread, QTimer, pyqtSignal
from PyQt5.QtGui import QFont
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget,
    QVBoxLayout, QHBoxLayout, QGridLayout,
    QLabel, QLineEdit, QPushButton, QRadioButton,
    QButtonGroup, QPlainTextEdit, QGroupBox, QFrame,
    QSizePolicy, QMessageBox,
)


# ---------------------------------------------------------------------------
# Paths (auto-detect frozen vs source)
# ---------------------------------------------------------------------------
if getattr(sys, "frozen", False):
    # Running as a built executable (PyInstaller bundle)
    PROJECT_ROOT = Path(sys.executable).parent.resolve()
    PYTHON_EXE = None  # not used in frozen mode
    PIPELINE_LAUNCH = [str(PROJECT_ROOT / "kafka_main.exe")]
else:
    # Running from source -- use the venv Python next to the script
    PROJECT_ROOT = Path(__file__).parent.resolve()
    venv_python = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"
    PYTHON_EXE = str(venv_python) if venv_python.exists() else sys.executable
    PIPELINE_LAUNCH = [PYTHON_EXE, str(PROJECT_ROOT / "kafka_main.py")]

PRESETS_PATH = PROJECT_ROOT / "LauncherPresets.json"
CONFIG_PATH = PROJECT_ROOT / "DataExporterConfig.json"
LOG_PATH = PROJECT_ROOT / "dis-kafka.log"

# Kafka install (assumed C:\kafka, Java auto-detected from common paths)
KAFKA_HOME = Path("C:/kafka")
KAFKA_START_BAT = KAFKA_HOME / "bin" / "windows" / "kafka-server-start.bat"
KAFKA_SERVER_PROPS = KAFKA_HOME / "config" / "kraft" / "server.properties"


def _find_java_home():
    """Locate a Java 17 install on Windows. Returns the JDK root path or None."""
    candidates = [
        os.environ.get("JAVA_HOME", ""),
        r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot",
        r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18-hotspot",
    ]
    # Also scan Adoptium dir for any jdk-17.* folder
    adoptium = Path(r"C:\Program Files\Eclipse Adoptium")
    if adoptium.exists():
        for p in sorted(adoptium.iterdir(), reverse=True):
            if p.is_dir() and p.name.startswith("jdk-17"):
                candidates.append(str(p))
    for c in candidates:
        if c and Path(c).exists() and (Path(c) / "bin" / "java.exe").exists():
            return c
    return None


JAVA_HOME = _find_java_home() or r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
DEFAULT_PRESETS = {
    "dev":  {"exercise_id": 9,  "port": 3000, "logger_file": "exp_dev_1.lzma",  "database_name": "noamtest"},
    "prod": {"exercise_id": 21, "port": 3001, "logger_file": "exp_prod_1.lzma", "database_name": "prod_db"},
    "_meta": {"theme": "light", "active_mode": "prod"},
}

KAFKA_PORT = 9092


# ---------------------------------------------------------------------------
# Stylesheets -- light and dark
# ---------------------------------------------------------------------------
LIGHT_STYLE = """
QMainWindow, QWidget { background-color: #f5f6fa; color: #2c3e50; }
QGroupBox { background-color: white; border: 1px solid #dcdde1; border-radius: 8px;
    margin-top: 16px; padding: 16px 14px 12px 14px; font-weight: bold;
    color: #2c3e50; font-size: 11pt; }
QGroupBox::title { subcontrol-origin: margin; subcontrol-position: top left;
    padding: 0 8px; color: #1a4d7c; background-color: white; }
QLabel { color: #2c3e50; font-weight: normal; font-size: 10pt; background: transparent; }
QLineEdit { border: 1px solid #ced6e0; border-radius: 4px; padding: 6px 8px;
    background: white; color: #2c3e50; selection-background-color: #1a4d7c; font-size: 10pt; }
QLineEdit:focus { border: 2px solid #1a4d7c; padding: 5px 7px; }
QLineEdit:disabled { background: #f1f2f6; color: #95a5a6; }
QPushButton { background-color: #34495e; color: white; border: none; border-radius: 4px;
    padding: 8px 16px; font-weight: 500; font-size: 10pt; }
QPushButton:hover { background-color: #1a4d7c; }
QPushButton:disabled { background-color: #b2bec3; }
QPushButton#themeBtn { background: transparent; color: #1a4d7c; font-size: 16pt;
    padding: 4px 12px; border: 1px solid #dcdde1; border-radius: 18px; }
QPushButton#themeBtn:hover { background: #ecf0f1; }
QPushButton#start { background-color: #27ae60; font-size: 14pt; padding: 16px; font-weight: bold; }
QPushButton#start:hover { background-color: #229954; }
QPushButton#start:disabled { background-color: #bdc3c7; }
QPushButton#stop { background-color: #c0392b; font-size: 14pt; padding: 16px; font-weight: bold; }
QPushButton#stop:hover { background-color: #a93226; }
QPushButton#stop:disabled { background-color: #bdc3c7; }
QRadioButton { color: #2c3e50; font-size: 11pt; font-weight: bold; spacing: 6px; background: transparent; }
QRadioButton::indicator { width: 18px; height: 18px; }
QPlainTextEdit { background-color: #2c3e50; color: #ecf0f1; border: 1px solid #34495e;
    border-radius: 6px; padding: 8px; font-family: "Consolas", "Courier New", monospace; font-size: 9pt; }
QFrame#runningBanner { background-color: white; border: 2px solid #27ae60; border-radius: 8px; }
"""

DARK_STYLE = """
QMainWindow, QWidget { background-color: #1e1e2e; color: #cdd6f4; }
QGroupBox { background-color: #2a2a3a; border: 1px solid #45475a; border-radius: 8px;
    margin-top: 16px; padding: 16px 14px 12px 14px; font-weight: bold;
    color: #cdd6f4; font-size: 11pt; }
QGroupBox::title { subcontrol-origin: margin; subcontrol-position: top left;
    padding: 0 8px; color: #89b4fa; background-color: #2a2a3a; }
QLabel { color: #cdd6f4; font-weight: normal; font-size: 10pt; background: transparent; }
QLineEdit { border: 1px solid #45475a; border-radius: 4px; padding: 6px 8px;
    background: #1e1e2e; color: #cdd6f4; selection-background-color: #89b4fa;
    selection-color: #1e1e2e; font-size: 10pt; }
QLineEdit:focus { border: 2px solid #89b4fa; padding: 5px 7px; }
QLineEdit:disabled { background: #181825; color: #6c7086; }
QPushButton { background-color: #45475a; color: #cdd6f4; border: none; border-radius: 4px;
    padding: 8px 16px; font-weight: 500; font-size: 10pt; }
QPushButton:hover { background-color: #585b70; }
QPushButton:disabled { background-color: #313244; color: #6c7086; }
QPushButton#themeBtn { background: transparent; color: #89b4fa; font-size: 16pt;
    padding: 4px 12px; border: 1px solid #45475a; border-radius: 18px; }
QPushButton#themeBtn:hover { background: #313244; }
QPushButton#start { background-color: #a6e3a1; color: #1e1e2e; font-size: 14pt; padding: 16px; font-weight: bold; }
QPushButton#start:hover { background-color: #94e294; }
QPushButton#start:disabled { background-color: #45475a; color: #6c7086; }
QPushButton#stop { background-color: #f38ba8; color: #1e1e2e; font-size: 14pt; padding: 16px; font-weight: bold; }
QPushButton#stop:hover { background-color: #e87295; }
QPushButton#stop:disabled { background-color: #45475a; color: #6c7086; }
QRadioButton { color: #cdd6f4; font-size: 11pt; font-weight: bold; spacing: 6px; background: transparent; }
QRadioButton::indicator { width: 18px; height: 18px; }
QPlainTextEdit { background-color: #11111b; color: #cdd6f4; border: 1px solid #45475a;
    border-radius: 6px; padding: 8px; font-family: "Consolas", "Courier New", monospace; font-size: 9pt; }
QFrame#runningBanner { background-color: #2a2a3a; border: 2px solid #a6e3a1; border-radius: 8px; }
"""


# ---------------------------------------------------------------------------
# Status indicator
# ---------------------------------------------------------------------------
class StatusDot(QLabel):
    STATES = {
        "up":       ("#27ae60", "UP"),
        "down":     ("#c0392b", "DOWN"),
        "unknown":  ("#95a5a6", "UNKNOWN"),
        "starting": ("#f39c12", "STARTING..."),
    }

    def __init__(self, label_text):
        super().__init__()
        self.label_text = label_text
        self.set_state("unknown")

    def set_state(self, state):
        color, text = self.STATES.get(state, self.STATES["unknown"])
        self.setText(
            f'<span style="color:{color};font-size:18pt;">●</span> '
            f'<b style="font-size:11pt;">{self.label_text}</b> '
            f'<span style="font-size:9pt;opacity:0.7;">({text})</span>'
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _port_open(host, port, timeout=1.0):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _check_sql(sql_server, db_name, timeout=3):
    """Returns True if SQL Server reachable and the configured DB exists."""
    try:
        import sqlalchemy
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={sql_server};DATABASE={db_name};Trusted_Connection=yes;"
            f"Connection Timeout={timeout};"
        )
        engine = sqlalchemy.create_engine(
            "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(conn_str),
            pool_pre_ping=True,
            connect_args={"timeout": timeout},
        )
        with engine.connect() as conn:
            n = conn.execute(sqlalchemy.text("SELECT 1")).scalar()
        engine.dispose()
        return n == 1
    except Exception:
        return False


def _read_active_config():
    """Best-effort read of DataExporterConfig.json. Returns dict or {}."""
    if not CONFIG_PATH.exists():
        return {}
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _ensure_topic_exists(broker, topic, num_partitions, log_emit):
    """Creates the topic if it does not exist. Returns True on success."""
    try:
        from confluent_kafka.admin import AdminClient, NewTopic
        admin = AdminClient({"bootstrap.servers": broker})
        meta = admin.list_topics(timeout=10.0)
        if topic in meta.topics:
            log_emit(f"Topic '{topic}' already exists")
            return True
        log_emit(f"Creating topic '{topic}' ({num_partitions} partitions)...")
        nt = NewTopic(topic, num_partitions=num_partitions, replication_factor=1)
        futures = admin.create_topics([nt], operation_timeout=15.0)
        for t, fut in futures.items():
            fut.result(timeout=15.0)
        log_emit(f"Topic '{topic}' created")
        return True
    except Exception as exc:
        log_emit(f"ERROR creating topic: {exc}")
        return False


# ---------------------------------------------------------------------------
# FirePdu builder (DEV mode test injector)
# ---------------------------------------------------------------------------
def build_fire_pdu(event_number, exercise_id, ts):
    """
    Build a 96-byte DIS 7 FirePdu (PDU type 2). Mirrors the format produced by
    tools/send_fire_pdu.py.
    """
    PDU_LENGTH = 96
    return (
        struct.pack('!BBBBIHH', 7, exercise_id, 2, 2, ts, PDU_LENGTH, 0)
        + struct.pack('!HHHHHH', 1, 3101, 1, 1, 3101, 2)        # firing/target entityID
        + struct.pack('!HHH', 1, 3101, 100)                       # munitionExpendableID
        + struct.pack('!HHH', 1, 3101, event_number)              # eventID
        + struct.pack('!I', event_number)                         # fireMissionIndex
        + struct.pack('!ddd', 4429530.0, 3094568.0, 3320580.0)    # location
        + struct.pack('!BBHBBBBHHHH', 2, 2, 105, 1, 1, 1, 0, 1000, 100, 1, 1)  # descriptor
        + struct.pack('!fff', 100.0, 0.0, -50.0)                  # velocity
        + struct.pack('!f', 5000.0)                               # range
    )


# ---------------------------------------------------------------------------
# Worker: sends N FirePdus over UDP at a given rate
# ---------------------------------------------------------------------------
class FirePduSender(QThread):
    progress = pyqtSignal(int, int)   # (sent, total)
    done = pyqtSignal(int, float)     # (count, elapsed_seconds)
    failed = pyqtSignal(str)

    def __init__(self, host, port, exercise_id, count, rate_per_sec, parent=None):
        super().__init__(parent)
        self.host = host
        self.port = int(port)
        self.exercise_id = int(exercise_id)
        self.count = int(count)
        self.rate = max(0.1, float(rate_per_sec))
        self._running = True

    def stop(self):
        self._running = False

    def run(self):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8 * 1024 * 1024)
            base_ts = int(time.time()) & 0xFFFFFFFF
            period = 1.0 / self.rate
            t_start = time.perf_counter()
            update_every = max(1, self.count // 50)  # ~50 progress events max

            for i in range(1, self.count + 1):
                if not self._running:
                    break
                pkt = build_fire_pdu(i, self.exercise_id, (base_ts + i) & 0xFFFFFFFF)
                sock.sendto(pkt, (self.host, self.port))
                if i % update_every == 0 or i == self.count:
                    self.progress.emit(i, self.count)
                # Smooth rate using deadline
                deadline = t_start + i * period
                wait = deadline - time.perf_counter()
                if wait > 0:
                    time.sleep(wait)
            sock.close()
            elapsed = time.perf_counter() - t_start
            self.done.emit(min(self.count, i), elapsed)
        except Exception as exc:
            self.failed.emit(str(exc))


# ---------------------------------------------------------------------------
# Worker: tails dis-kafka.log line by line
# ---------------------------------------------------------------------------
class LogTailWorker(QThread):
    new_line = pyqtSignal(str)

    def __init__(self, log_path, parent=None):
        super().__init__(parent)
        self.log_path = log_path
        self._running = True

    def stop(self):
        self._running = False

    def run(self):
        # Wait for log to appear
        while self._running and not self.log_path.exists():
            time.sleep(0.5)
        if not self._running:
            return
        try:
            with open(self.log_path, "r", encoding="utf-8", errors="replace") as f:
                f.seek(0, os.SEEK_END)
                while self._running:
                    line = f.readline()
                    if line:
                        self.new_line.emit(line.rstrip("\n"))
                    else:
                        time.sleep(0.2)
        except Exception as exc:
            self.new_line.emit(f"[log-tail] error: {exc}")


# ---------------------------------------------------------------------------
# Worker: periodic status polling
# ---------------------------------------------------------------------------
class StatusPoller(QThread):
    kafka_state = pyqtSignal(str)
    sql_state = pyqtSignal(str)

    def __init__(self, interval=5.0, parent=None):
        super().__init__(parent)
        self.interval = interval
        self._running = True

    def stop(self):
        self._running = False

    def run(self):
        while self._running:
            # Kafka
            self.kafka_state.emit("up" if _port_open("localhost", KAFKA_PORT, 1.0) else "down")
            # SQL
            cfg = _read_active_config()
            sql_server = cfg.get("sql_server", r"localhost\SQLEXPRESS")
            db_name = cfg.get("database_name", "noamtest")
            self.sql_state.emit("up" if _check_sql(sql_server, db_name, 2) else "down")
            # Sleep cooperatively
            for _ in range(int(self.interval * 5)):
                if not self._running:
                    return
                time.sleep(0.2)


# ---------------------------------------------------------------------------
# Worker: orchestrates the start sequence
# ---------------------------------------------------------------------------
class StartSequenceWorker(QThread):
    progress = pyqtSignal(str)
    status_update = pyqtSignal(str, str)  # component, state
    finished_ok = pyqtSignal(int, int)     # kafka_pid, pipeline_pid
    finished_fail = pyqtSignal(str)

    def __init__(self, active_mode, active_preset, parent=None):
        super().__init__(parent)
        self.active_mode = active_mode
        self.active_preset = active_preset
        self.kafka_proc = None
        self.pipeline_proc = None

    def run(self):
        try:
            # Step 1: write active preset into DataExporterConfig.json
            self.progress.emit(f"Writing active preset ({self.active_mode.upper()}) to DataExporterConfig.json")
            cfg = _read_active_config()
            cfg["exercise_id"]   = self.active_preset["exercise_id"]
            cfg["PORT"]          = self.active_preset["port"]
            cfg["logger_file"]   = self.active_preset["logger_file"]
            cfg["database_name"] = self.active_preset["database_name"]
            # Force fresh-start mode so each scenario starts with a clean Kafka view
            cfg.setdefault("kafka", {})
            cfg["kafka"]["reset_topic_on_startup"] = True
            with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                json.dump(cfg, f, indent=4)

            # Truncate the live log so we only see this run's output
            try:
                LOG_PATH.unlink(missing_ok=True)
            except Exception:
                pass

            # Step 2: start Kafka if not already running
            if _port_open("localhost", KAFKA_PORT, 0.5):
                self.progress.emit("Kafka broker already running on port 9092 -- reusing")
                self.status_update.emit("kafka", "up")
            else:
                self.progress.emit("Starting Kafka broker...")
                self.status_update.emit("kafka", "starting")
                if not KAFKA_START_BAT.exists():
                    self.finished_fail.emit(f"Kafka not found at {KAFKA_HOME} -- install required")
                    return
                if not Path(JAVA_HOME).exists():
                    self.finished_fail.emit(f"Java JDK 17 not found -- install required")
                    return
                env = os.environ.copy()
                env["JAVA_HOME"] = JAVA_HOME
                env["PATH"] = str(Path(JAVA_HOME) / "bin") + os.pathsep + env.get("PATH", "")
                self.kafka_proc = subprocess.Popen(
                    [str(KAFKA_START_BAT), str(KAFKA_SERVER_PROPS)],
                    env=env,
                    cwd=str(KAFKA_HOME),
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
                )
                # Wait up to 90s for port 9092
                deadline = time.monotonic() + 90.0
                while time.monotonic() < deadline:
                    if _port_open("localhost", KAFKA_PORT, 0.5):
                        break
                    if self.kafka_proc.poll() is not None:
                        self.finished_fail.emit("Kafka broker exited unexpectedly during start")
                        return
                    time.sleep(1.0)
                else:
                    self.finished_fail.emit("Kafka broker did not become ready in 90s")
                    return
                self.progress.emit("Kafka broker is up on port 9092")
                self.status_update.emit("kafka", "up")

            # Step 3: ensure topic dis.raw exists
            kcfg = cfg.get("kafka", {})
            topic = kcfg.get("topic", "dis.raw")
            partitions = int(kcfg.get("num_partitions", 4))
            if not _ensure_topic_exists(f"localhost:{KAFKA_PORT}", topic, partitions, self.progress.emit):
                self.finished_fail.emit("Could not ensure Kafka topic exists")
                return

            # Step 4: SQL pre-check (best effort)
            sql_server = cfg.get("sql_server", r"localhost\SQLEXPRESS")
            db_name = cfg.get("database_name", "noamtest")
            if _check_sql(sql_server, db_name, 3):
                self.progress.emit(f"SQL Server reachable ({sql_server}, db={db_name})")
                self.status_update.emit("sql", "up")
            else:
                self.progress.emit(f"WARNING: SQL Server unreachable ({sql_server}, db={db_name}) -- pipeline will retry")
                self.status_update.emit("sql", "down")

            # Step 5: spawn kafka_main.py
            self.progress.emit("Starting pipeline (kafka_main.py)...")
            self.status_update.emit("pipeline", "starting")
            self.pipeline_proc = subprocess.Popen(
                PIPELINE_LAUNCH,
                cwd=str(PROJECT_ROOT),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
            )
            # Wait for "Listening UDP on port" in dis-kafka.log
            target = "Listening UDP on port"
            deadline = time.monotonic() + 60.0
            seen = False
            while time.monotonic() < deadline:
                if LOG_PATH.exists():
                    try:
                        with open(LOG_PATH, "r", encoding="utf-8", errors="replace") as f:
                            if target in f.read():
                                seen = True
                                break
                    except Exception:
                        pass
                if self.pipeline_proc.poll() is not None:
                    self.finished_fail.emit("Pipeline (kafka_main.py) exited unexpectedly during start")
                    return
                time.sleep(0.5)

            if not seen:
                self.finished_fail.emit("Pipeline did not become ready in 60s (no 'Listening UDP' in dis-kafka.log)")
                return

            self.progress.emit("Pipeline is up and listening")
            self.status_update.emit("pipeline", "up")

            kafka_pid = self.kafka_proc.pid if self.kafka_proc else 0
            pipeline_pid = self.pipeline_proc.pid if self.pipeline_proc else 0
            self.finished_ok.emit(kafka_pid, pipeline_pid)

        except Exception as exc:
            self.finished_fail.emit(f"Unexpected error during start: {exc}")


# ---------------------------------------------------------------------------
# Worker: orchestrates graceful shutdown
# ---------------------------------------------------------------------------
class StopSequenceWorker(QThread):
    progress = pyqtSignal(str)
    finished_done = pyqtSignal()

    def __init__(self, kafka_proc, pipeline_proc, parent=None):
        super().__init__(parent)
        self.kafka_proc = kafka_proc
        self.pipeline_proc = pipeline_proc

    def _graceful_stop(self, proc, name, timeout=30):
        if proc is None or proc.poll() is not None:
            self.progress.emit(f"{name} already stopped")
            return
        self.progress.emit(f"Sending Ctrl+Break to {name} (PID {proc.pid})...")
        try:
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        except Exception as exc:
            self.progress.emit(f"send_signal failed: {exc} -- falling back to terminate")
            try:
                proc.terminate()
            except Exception:
                pass
        try:
            proc.wait(timeout=timeout)
            self.progress.emit(f"{name} terminated cleanly")
        except subprocess.TimeoutExpired:
            self.progress.emit(f"{name} did not stop in {timeout}s -- forcing kill")
            try:
                proc.kill()
                proc.wait(timeout=5)
            except Exception:
                pass

    def run(self):
        # Stop pipeline first (so it can drain backlog and commit)
        self._graceful_stop(self.pipeline_proc, "kafka_main.py", timeout=30)
        # Then Kafka
        self._graceful_stop(self.kafka_proc, "Kafka broker", timeout=30)
        self.finished_done.emit()


# ---------------------------------------------------------------------------
# Main launcher window
# ---------------------------------------------------------------------------
class LauncherWindow(QMainWindow):

    STATE_IDLE = "idle"
    STATE_STARTING = "starting"
    STATE_RUNNING = "running"
    STATE_STOPPING = "stopping"

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Data Exporter Launcher")
        self.resize(900, 720)

        self.presets = self._load_presets()
        self.theme = self.presets.get("_meta", {}).get("theme", "light")
        self.active_mode = self.presets.get("_meta", {}).get("active_mode", "prod")
        self.ui_state = self.STATE_IDLE

        # Worker / process handles
        self.kafka_proc = None
        self.pipeline_proc = None
        self.start_worker = None
        self.stop_worker = None
        self.log_tail = None

        self._build_ui()
        self._populate_fields_for_mode(self.active_mode)
        self._apply_theme()

        # Status poller (runs continuously)
        self.poller = StatusPoller(interval=5.0)
        self.poller.kafka_state.connect(self._on_kafka_state)
        self.poller.sql_state.connect(self._on_sql_state)
        self.poller.start()

    # --- Persistence ---

    def _load_presets(self):
        if PRESETS_PATH.exists():
            try:
                with open(PRESETS_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
                for k in ("dev", "prod", "_meta"):
                    if k not in data:
                        data[k] = DEFAULT_PRESETS[k]
                return data
            except Exception:
                pass
        self._save_presets(DEFAULT_PRESETS)
        return dict(DEFAULT_PRESETS)

    def _save_presets(self, presets):
        with open(PRESETS_PATH, "w", encoding="utf-8") as f:
            json.dump(presets, f, indent=4)

    def _capture_active_preset_into_memory(self):
        self.presets[self.active_mode] = {
            "exercise_id":   self._safe_int(self.fld_exercise.text(),
                                            DEFAULT_PRESETS[self.active_mode]["exercise_id"]),
            "port":          self._safe_int(self.fld_port.text(),
                                            DEFAULT_PRESETS[self.active_mode]["port"]),
            "logger_file":   self.fld_logger.text().strip()
                             or DEFAULT_PRESETS[self.active_mode]["logger_file"],
            "database_name": self.fld_db.text().strip()
                             or DEFAULT_PRESETS[self.active_mode]["database_name"],
        }

    @staticmethod
    def _safe_int(s, default):
        try:
            return int(str(s).strip())
        except Exception:
            return default

    # --- UI construction ---

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setContentsMargins(20, 20, 20, 20)
        root.setSpacing(12)

        # Header
        header = QHBoxLayout()
        self.title_label = QLabel()
        header.addWidget(self.title_label)
        header.addStretch()

        self.theme_btn = QPushButton("☾")
        self.theme_btn.setObjectName("themeBtn")
        self.theme_btn.setFixedWidth(50)
        self.theme_btn.setToolTip("Toggle light / dark theme")
        self.theme_btn.clicked.connect(self._on_toggle_theme)
        header.addWidget(self.theme_btn)
        header.addSpacing(20)

        self.mode_label = QLabel()
        header.addWidget(self.mode_label)
        self.dev_radio = QRadioButton("DEV")
        self.prod_radio = QRadioButton("PROD")
        self.mode_group = QButtonGroup(self)
        self.mode_group.addButton(self.dev_radio)
        self.mode_group.addButton(self.prod_radio)
        if self.active_mode == "dev":
            self.dev_radio.setChecked(True)
        else:
            self.prod_radio.setChecked(True)
        self.dev_radio.toggled.connect(self._on_mode_change)
        header.addWidget(self.dev_radio)
        header.addWidget(self.prod_radio)
        root.addLayout(header)

        # Status row
        self.status_box = QGroupBox("Status")
        status_layout = QHBoxLayout()
        self.status_box.setLayout(status_layout)
        self.kafka_dot = StatusDot("Kafka broker")
        self.sql_dot = StatusDot("SQL Server")
        self.pipe_dot = StatusDot("Pipeline")
        for dot in (self.kafka_dot, self.sql_dot, self.pipe_dot):
            status_layout.addWidget(dot)
            status_layout.addStretch()
        root.addWidget(self.status_box)

        # IDLE: config panel
        self.cfg_box = QGroupBox(f"{self.active_mode.upper()} preset")
        cfg_grid = QGridLayout()
        self.cfg_box.setLayout(cfg_grid)
        cfg_grid.setColumnStretch(1, 1)
        cfg_grid.setVerticalSpacing(10)
        cfg_grid.setHorizontalSpacing(15)
        cfg_grid.addWidget(QLabel("exercise_id"),   0, 0)
        cfg_grid.addWidget(QLabel("port (UDP)"),    1, 0)
        cfg_grid.addWidget(QLabel("logger_file"),   2, 0)
        cfg_grid.addWidget(QLabel("database_name"), 3, 0)
        self.fld_exercise = QLineEdit()
        self.fld_port = QLineEdit()
        self.fld_logger = QLineEdit()
        self.fld_db = QLineEdit()
        cfg_grid.addWidget(self.fld_exercise, 0, 1)
        cfg_grid.addWidget(self.fld_port,     1, 1)
        cfg_grid.addWidget(self.fld_logger,   2, 1)
        cfg_grid.addWidget(self.fld_db,       3, 1)
        hint = QLabel(
            "<span style='color:#95a5a6;font-size:9pt;'>"
            "Switch above to edit the other preset. Both presets are stored separately in "
            "<code>LauncherPresets.json</code>."
            "</span>"
        )
        hint.setWordWrap(True)
        cfg_grid.addWidget(hint, 4, 0, 1, 2)
        root.addWidget(self.cfg_box)

        self.cfg_actions_widget = QWidget()
        cfg_actions = QHBoxLayout()
        self.cfg_actions_widget.setLayout(cfg_actions)
        cfg_actions.setContentsMargins(0, 0, 0, 0)
        save_btn = QPushButton("\U0001F4BE  Save preset")
        save_btn.clicked.connect(self._on_save_presets)
        adv_btn = QPushButton("\U0001F4DD  Edit advanced config (DataExporterConfig.json)...")
        adv_btn.clicked.connect(self._on_edit_advanced)
        cfg_actions.addWidget(save_btn)
        cfg_actions.addStretch()
        cfg_actions.addWidget(adv_btn)
        root.addWidget(self.cfg_actions_widget)

        self.start_btn = QPushButton("▶  START EVERYTHING")
        self.start_btn.setObjectName("start")
        self.start_btn.clicked.connect(self._on_start)
        root.addWidget(self.start_btn)

        # RUNNING: banner
        self.running_banner = QFrame()
        self.running_banner.setObjectName("runningBanner")
        rb_layout = QHBoxLayout()
        self.running_banner.setLayout(rb_layout)
        rb_layout.setContentsMargins(20, 16, 20, 16)
        rb_info = QVBoxLayout()
        self.running_title = QLabel()
        rb_info.addWidget(self.running_title)
        self.running_logger = QLabel("Logger : ...")
        self.running_logger.setStyleSheet("font-size: 11pt;")
        rb_info.addWidget(self.running_logger)
        self.running_exercise = QLabel("Exercise ID : ... | UDP port : ... | Mode : ...")
        self.running_exercise.setStyleSheet("font-size: 10pt;")
        rb_info.addWidget(self.running_exercise)
        rb_layout.addLayout(rb_info)
        rb_layout.addStretch()
        self.stop_btn = QPushButton("■  STOP")
        self.stop_btn.setObjectName("stop")
        self.stop_btn.setFixedWidth(180)
        self.stop_btn.clicked.connect(self._on_stop)
        rb_layout.addWidget(self.stop_btn)
        root.addWidget(self.running_banner)
        self.running_banner.hide()

        # DEV tools (visible only when DEV active + pipeline running)
        self.dev_tools_box = QGroupBox("DEV tools  (test injection)")
        dev_grid = QGridLayout()
        self.dev_tools_box.setLayout(dev_grid)
        dev_grid.setColumnStretch(1, 0)
        dev_grid.setColumnStretch(3, 0)
        dev_grid.setColumnStretch(5, 1)

        dev_grid.addWidget(QLabel("Send"), 0, 0)
        self.fld_send_count = QLineEdit("100")
        self.fld_send_count.setFixedWidth(80)
        dev_grid.addWidget(self.fld_send_count, 0, 1)
        dev_grid.addWidget(QLabel("FirePdus at"), 0, 2)
        self.fld_send_rate = QLineEdit("50")
        self.fld_send_rate.setFixedWidth(80)
        dev_grid.addWidget(self.fld_send_rate, 0, 3)
        dev_grid.addWidget(QLabel("msg/sec"), 0, 4)
        dev_grid.addWidget(QWidget(), 0, 5)  # spacer

        self.send_btn = QPushButton("▶  Send PDUs")
        self.send_btn.clicked.connect(self._on_send_pdus)
        dev_grid.addWidget(self.send_btn, 0, 6)

        self.send_progress_label = QLabel(
            "<span style='color:#7f8c8d;font-size:9pt;'>"
            "PDUs are sent over UDP to localhost on the active port."
            "</span>"
        )
        dev_grid.addWidget(self.send_progress_label, 1, 0, 1, 7)

        root.addWidget(self.dev_tools_box)
        self.dev_tools_box.hide()

        # Sender worker placeholder
        self.sender = None

        # Live log
        log_box = QGroupBox("Live log")
        log_layout = QVBoxLayout()
        log_box.setLayout(log_layout)
        self.log_area = QPlainTextEdit()
        self.log_area.setReadOnly(True)
        self.log_area.setMaximumBlockCount(5000)
        self.log_area.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        font = QFont("Consolas", 9)
        self.log_area.setFont(font)
        log_layout.addWidget(self.log_area)
        root.addWidget(log_box, stretch=1)

        # Footer
        self.footer_label = QLabel(
            f"<span style='font-size:8pt;'>"
            f"Presets: {PRESETS_PATH}<br>"
            f"Config:&nbsp;&nbsp;{CONFIG_PATH}<br>"
            f"Pipeline log: {LOG_PATH}"
            f"</span>"
        )
        self.footer_label.setStyleSheet("color: #95a5a6;")
        root.addWidget(self.footer_label)

        self._log("Launcher ready. Click ▶ START EVERYTHING to launch Kafka + pipeline.")

    def _populate_fields_for_mode(self, mode):
        d = self.presets.get(mode, DEFAULT_PRESETS[mode])
        self.fld_exercise.setText(str(d.get("exercise_id", DEFAULT_PRESETS[mode]["exercise_id"])))
        self.fld_port.setText(str(d.get("port", DEFAULT_PRESETS[mode]["port"])))
        self.fld_logger.setText(str(d.get("logger_file", DEFAULT_PRESETS[mode]["logger_file"])))
        self.fld_db.setText(str(d.get("database_name", DEFAULT_PRESETS[mode]["database_name"])))
        self.cfg_box.setTitle(f"{mode.upper()} preset")

    # --- Theme ---

    def _apply_theme(self):
        if self.theme == "dark":
            self.setStyleSheet(DARK_STYLE)
            self.theme_btn.setText("☀")
            self.theme_btn.setToolTip("Switch to light theme")
            self.title_label.setText(
                "<h1 style='color:#89b4fa;margin:0;'>Data Exporter Launcher</h1>"
                "<span style='color:#a6adc8;font-size:9pt;'>"
                "DIS &rarr; Kafka &rarr; SQL Server pipeline"
                "</span>"
            )
            self.mode_label.setText("<b style='font-size:12pt;color:#89b4fa;'>Mode :</b>")
            self.running_title.setText(
                "<span style='color:#a6e3a1;font-size:14pt;font-weight:bold;'>"
                "⚡ PIPELINE RUNNING</span>"
            )
        else:
            self.setStyleSheet(LIGHT_STYLE)
            self.theme_btn.setText("☾")
            self.theme_btn.setToolTip("Switch to dark theme")
            self.title_label.setText(
                "<h1 style='color:#1a4d7c;margin:0;'>Data Exporter Launcher</h1>"
                "<span style='color:#7f8c8d;font-size:9pt;'>"
                "DIS &rarr; Kafka &rarr; SQL Server pipeline"
                "</span>"
            )
            self.mode_label.setText("<b style='font-size:12pt;color:#1a4d7c;'>Mode :</b>")
            self.running_title.setText(
                "<span style='color:#27ae60;font-size:14pt;font-weight:bold;'>"
                "⚡ PIPELINE RUNNING</span>"
            )

    def _on_toggle_theme(self):
        self.theme = "dark" if self.theme == "light" else "light"
        self.presets.setdefault("_meta", {})["theme"] = self.theme
        self._save_presets(self.presets)
        self._apply_theme()
        self._log(f"Theme switched to {self.theme.upper()}")

    # --- IDLE / RUNNING state switching ---

    def _enter_running_state(self):
        self.ui_state = self.STATE_RUNNING
        self.cfg_box.hide()
        self.cfg_actions_widget.hide()
        self.start_btn.hide()
        self.dev_radio.setEnabled(False)
        self.prod_radio.setEnabled(False)
        active = self.presets[self.active_mode]
        self.running_logger.setText(
            f"<b>Logger :</b> {active['logger_file']} &nbsp;&nbsp;|&nbsp;&nbsp; "
            f"<b>Database :</b> {active.get('database_name', '?')}"
        )
        self.running_exercise.setText(
            f"<b>Exercise ID :</b> {active['exercise_id']} &nbsp;&nbsp;|&nbsp;&nbsp; "
            f"<b>UDP port :</b> {active['port']} &nbsp;&nbsp;|&nbsp;&nbsp; "
            f"<b>Mode :</b> {self.active_mode.upper()}"
        )
        self.running_banner.show()
        self.stop_btn.setEnabled(True)

        # DEV tools panel: show only in DEV mode
        if self.active_mode == "dev":
            self.send_btn.setEnabled(True)
            self.send_progress_label.setText(
                "<span style='color:#7f8c8d;font-size:9pt;'>"
                f"PDUs are sent over UDP to localhost:{active['port']} with "
                f"exerciseID={active['exercise_id']}."
                "</span>"
            )
            self.dev_tools_box.show()
        else:
            self.dev_tools_box.hide()

    def _enter_idle_state(self):
        self.ui_state = self.STATE_IDLE
        self.running_banner.hide()
        self.dev_tools_box.hide()
        self.cfg_box.show()
        self.cfg_actions_widget.show()
        self.start_btn.show()
        self.start_btn.setEnabled(True)
        self.dev_radio.setEnabled(True)
        self.prod_radio.setEnabled(True)

    def _log(self, msg):
        ts = time.strftime("%H:%M:%S")
        self.log_area.appendPlainText(f"[{ts}] {msg}")

    # --- Slots ---

    def _on_mode_change(self):
        if self.ui_state != self.STATE_IDLE:
            return
        self._capture_active_preset_into_memory()
        self.active_mode = "dev" if self.dev_radio.isChecked() else "prod"
        self.presets.setdefault("_meta", {})["active_mode"] = self.active_mode
        self._populate_fields_for_mode(self.active_mode)
        self._save_presets(self.presets)
        self._log(f"Active mode -> {self.active_mode.upper()}")

    def _on_save_presets(self):
        self._capture_active_preset_into_memory()
        self.presets.setdefault("_meta", {})["active_mode"] = self.active_mode
        self._save_presets(self.presets)
        self._log(f"Preset {self.active_mode.upper()} saved")

    def _on_edit_advanced(self):
        if not CONFIG_PATH.exists():
            QMessageBox.warning(self, "File not found",
                                f"DataExporterConfig.json does not exist at:\n{CONFIG_PATH}")
            return
        try:
            os.startfile(str(CONFIG_PATH))
            self._log(f"Opened {CONFIG_PATH.name} in associated editor")
        except Exception as exc:
            QMessageBox.warning(self, "Could not open file", str(exc))

    def _on_start(self):
        if self.ui_state != self.STATE_IDLE:
            return
        self.ui_state = self.STATE_STARTING
        self.start_btn.setEnabled(False)
        self.dev_radio.setEnabled(False)
        self.prod_radio.setEnabled(False)
        self._capture_active_preset_into_memory()
        self._save_presets(self.presets)

        active_preset = self.presets[self.active_mode]
        self._log("=" * 60)
        self._log(f"START sequence -- mode {self.active_mode.upper()}, "
                  f"exercise_id={active_preset['exercise_id']}, "
                  f"port={active_preset['port']}, "
                  f"logger_file={active_preset['logger_file']}")

        # Launch start worker
        self.start_worker = StartSequenceWorker(self.active_mode, active_preset)
        self.start_worker.progress.connect(lambda m: self._log(m))
        self.start_worker.status_update.connect(self._on_status_update)
        self.start_worker.finished_ok.connect(self._on_start_ok)
        self.start_worker.finished_fail.connect(self._on_start_fail)
        self.start_worker.start()

    def _on_start_ok(self, kafka_pid, pipeline_pid):
        # Grab proc handles from the worker (it owns them)
        self.kafka_proc = self.start_worker.kafka_proc
        self.pipeline_proc = self.start_worker.pipeline_proc

        self._log(f"All systems UP -- kafka_pid={kafka_pid or 'reused'} pipeline_pid={pipeline_pid}")
        self._enter_running_state()

        # Start tailing dis-kafka.log
        self.log_tail = LogTailWorker(LOG_PATH)
        self.log_tail.new_line.connect(lambda line: self.log_area.appendPlainText(line))
        self.log_tail.start()

    def _on_start_fail(self, msg):
        self._log(f"START FAILED: {msg}")
        QMessageBox.critical(self, "Start failed", msg)
        # Try to clean up anything partial
        if self.start_worker:
            for proc in (self.start_worker.pipeline_proc, self.start_worker.kafka_proc):
                if proc and proc.poll() is None:
                    try:
                        proc.terminate()
                    except Exception:
                        pass
        self._enter_idle_state()

    def _on_stop(self):
        if self.ui_state != self.STATE_RUNNING:
            return
        self.ui_state = self.STATE_STOPPING
        self.stop_btn.setEnabled(False)
        self._log("=" * 60)
        self._log("STOP sequence -- pipeline first, then Kafka")

        # Stop log tail
        if self.log_tail:
            self.log_tail.stop()
            self.log_tail.wait(2000)
            self.log_tail = None

        self.stop_worker = StopSequenceWorker(self.kafka_proc, self.pipeline_proc)
        self.stop_worker.progress.connect(lambda m: self._log(m))
        self.stop_worker.finished_done.connect(self._on_stop_done)
        self.stop_worker.start()

    def _on_stop_done(self):
        self.kafka_proc = None
        self.pipeline_proc = None
        self.pipe_dot.set_state("down")
        self._log("STOP sequence complete")
        self._enter_idle_state()

    # --- DEV tools: PDU sender ---

    def _on_send_pdus(self):
        if self.ui_state != self.STATE_RUNNING or self.active_mode != "dev":
            return
        if self.sender is not None and self.sender.isRunning():
            self._log("A send is already in progress -- wait for it to finish.")
            return

        try:
            count = int(self.fld_send_count.text().strip())
            rate = float(self.fld_send_rate.text().strip())
        except ValueError:
            QMessageBox.warning(self, "Invalid input",
                                "Count must be an integer, rate must be a number (msg/sec).")
            return
        if count <= 0 or rate <= 0:
            QMessageBox.warning(self, "Invalid input",
                                "Count and rate must both be > 0.")
            return
        if count > 1_000_000:
            QMessageBox.warning(self, "Too many",
                                "Limit count to 1 000 000 to keep the launcher responsive.")
            return

        active = self.presets[self.active_mode]
        port = int(active["port"])
        exercise_id = int(active["exercise_id"])

        self.send_btn.setEnabled(False)
        self.send_progress_label.setText(
            f"<span style='color:#1a4d7c;font-size:9pt;'>"
            f"Sending {count} FirePdus at {rate:.1f} msg/sec to localhost:{port}..."
            f"</span>"
        )
        self._log(f"DEV: sending {count} FirePdus at {rate:.1f} msg/sec to localhost:{port}, exerciseID={exercise_id}")

        self.sender = FirePduSender("localhost", port, exercise_id, count, rate)
        self.sender.progress.connect(self._on_send_progress)
        self.sender.done.connect(self._on_send_done)
        self.sender.failed.connect(self._on_send_failed)
        self.sender.start()

    def _on_send_progress(self, sent, total):
        self.send_progress_label.setText(
            f"<span style='color:#1a4d7c;font-size:9pt;'>"
            f"Sending... {sent} / {total} ({100 * sent / total:.0f}%)"
            f"</span>"
        )

    def _on_send_done(self, count, elapsed):
        rate = count / elapsed if elapsed > 0 else 0
        self.send_progress_label.setText(
            f"<span style='color:#27ae60;font-size:9pt;'>"
            f"✓ Done. Sent {count} FirePdus in {elapsed:.2f}s ({rate:.0f} msg/sec actual)."
            f"</span>"
        )
        self._log(f"DEV: send complete -- {count} PDUs in {elapsed:.2f}s ({rate:.0f} msg/sec actual)")
        self.send_btn.setEnabled(True)

    def _on_send_failed(self, msg):
        self.send_progress_label.setText(
            f"<span style='color:#c0392b;font-size:9pt;'>✗ Send failed: {msg}</span>"
        )
        self._log(f"DEV: send FAILED -- {msg}")
        self.send_btn.setEnabled(True)

    # --- Status updates ---

    def _on_status_update(self, component, state):
        dot = {"kafka": self.kafka_dot, "sql": self.sql_dot, "pipeline": self.pipe_dot}.get(component)
        if dot:
            dot.set_state(state)

    def _on_kafka_state(self, state):
        # Don't override "starting" with stale "down"
        if self.ui_state == self.STATE_STARTING:
            return
        self.kafka_dot.set_state(state)

    def _on_sql_state(self, state):
        self.sql_dot.set_state(state)

    # --- Window close ---

    def closeEvent(self, event):
        if self.ui_state in (self.STATE_RUNNING, self.STATE_STARTING, self.STATE_STOPPING):
            ret = QMessageBox.question(
                self, "Pipeline running",
                "The pipeline is running. Stop it gracefully before closing?",
                QMessageBox.Yes | QMessageBox.No | QMessageBox.Cancel,
                QMessageBox.Yes,
            )
            if ret == QMessageBox.Cancel:
                event.ignore()
                return
            if ret == QMessageBox.Yes and self.ui_state == self.STATE_RUNNING:
                # Synchronous stop
                if self.log_tail:
                    self.log_tail.stop()
                    self.log_tail.wait(2000)
                worker = StopSequenceWorker(self.kafka_proc, self.pipeline_proc)
                worker.progress.connect(lambda m: print(m))
                worker.start()
                worker.wait(60000)
        # Stop the poller
        if self.poller:
            self.poller.stop()
            self.poller.wait(2000)
        event.accept()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    # Fix multiprocessing on Windows when frozen (PyInstaller)
    import multiprocessing
    multiprocessing.freeze_support()

    app = QApplication(sys.argv)
    win = LauncherWindow()
    win.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
