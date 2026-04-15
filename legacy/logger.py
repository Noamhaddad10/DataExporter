import warnings
import sys

sys.setrecursionlimit(sys.getrecursionlimit() * 5)
warnings.filterwarnings("ignore", category=UserWarning, module="pkg_resources")
import json
import lzma
import socket
import struct
import sys
import pickle

# sys.setrecursionlimit(sys.getrecursionlimit() * 10)
import datetime
import threading
import time
import traceback
import multiprocessing
import os
import shutil
from multiprocessing import Queue, Process, freeze_support, shared_memory, Lock
import subprocess
from LoggerSQLExporter import LoggerSQLExporter
from LoggerPduProcessor import LoggerPduProcessor, LoggerPDU
import atexit
import logging
import logging.handlers
import pandas as pd
import re
import pyodbc
import sqlalchemy
import urllib.parse
from contextlib import contextmanager

@contextmanager
def sqlConn(database_name: str):
    """Local replacement for BatLab.Tools.sqlConn.
    Returns a SQLAlchemy connection to the local SQL Server Express instance
    using Windows Authentication (Trusted_Connection=yes).
    Compatible with pandas.read_sql_query().
    """
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER=localhost\\SQLEXPRESS;"
        f"DATABASE={database_name};"
        f"Trusted_Connection=yes;"
    )
    engine = sqlalchemy.create_engine(
        "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(conn_str)
    )
    with engine.connect() as conn:
        yield conn

LOG_FORMAT = '%(asctime)s | %(processName)s(%(process)d) | %(levelname)s | %(name)s | %(message)s'


def start_log_listener(log_queue: Queue, log_file: str, level=logging.DEBUG):
    fmt = logging.Formatter(LOG_FORMAT)
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(fmt)

    listener = logging.handlers.QueueListener(
        log_queue,
        file_handler,
        respect_handler_level=True
    )
    listener.start()
    return listener


def configure_worker_logger(log_queue: Queue, level=logging.DEBUG):
    root = logging.getLogger()
    root.setLevel(level)

    root.handlers.clear()

    qh = logging.handlers.QueueHandler(log_queue)
    qh.setLevel(level)
    root.addHandler(qh)

    root.propagate = False


count_total = 0


def cleanup():
    """
    This function runs at the end of the program and cleans the temporary directory that PyInstaller creates. This is
    done because if the exe file isn't closed properly the temporary file is saved, and we need to delete it manually
    """
    if hasattr(sys, '_MEIPASS'):
        shutil.rmtree(sys._MEIPASS, ignore_errors=True)


atexit.register(cleanup)


class DataWriter:
    """
    This class writes compressed DIS PDUs to the chosen loggerfile.
    It is used with the `with` statement, to ensure the files are properly opened and closed
    """

    def __init__(self, output_file_name: str, experiment_dir: str, logger_dir: str, net_dir: str,
                 lzma_compressor: lzma.LZMACompressor,
                 output_writer=print):
        """
        :param output_file_name: str : name of the output file
        :param logger_dir: str : relative path from current directory to log storage
        :param lzma_compressor: lzma Compressor : The compressor for the file
        :param output_writer: function : Function to print information. Defaults to print()
        """
        self.output_file_name = output_file_name
        self.logger_dir = logger_dir
        self.experiment_dir = experiment_dir
        self.net_dir = net_dir
        self.lzc = lzma_compressor
        self.output_writer = output_writer
        if not os.path.exists(f"{self.logger_dir}/{self.experiment_dir}"):
            os.makedirs(f"{self.logger_dir}/{self.experiment_dir}", exist_ok=True)
            print('local lzma dir created')
        # These dividers are given slightly odd names. This is to ensure that they will not appear within a PDU
        # and confuse things due to a PDU being split in the middle
        # Divider between data on a single line
        self.line_divider = b"line_divider"
        # Divider between lines of data
        self.line_separator = b"line_separator"
        self.destination_dir = f"{self.net_dir}\\{self.experiment_dir}"
        if not os.path.exists(self.destination_dir):
            os.makedirs(self.destination_dir, exist_ok=True)
            print(f"directory created-{self.destination_dir}")
        self.output_file = None

    def __enter__(self):
        self.output_file = open(f"{self.logger_dir}/{self.experiment_dir}/{self.output_file_name}", 'ab')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.output_writer(f"Writer closed {exc_type} : {exc_val} : {exc_tb}")
        self.output_file.write(self.lzc.flush())
        self.output_file.close()
        try:
            shutil.move(f"{self.logger_dir}/{self.experiment_dir}/{self.output_file_name}", self.destination_dir)
        except shutil.Error as e:
            print(f"error has occurred while moving the lzma to files:{e}")
            log.error(f"error has occurred while moving the lzma to files:{e}")
            print("check if a file with this name exists or the path is wrong")

    def write(self, pdu_data: bytes, packettime: float):
        """
        This method compresses, and writes the provided PDU, and additional data to the logger file.
        :param pdu_data: bytes
        :param packettime: float
        :return: None
        """
        bytes_packettime = struct.pack("d", packettime)
        self.output_file.write(
            self.lzc.compress(
                pdu_data + self.line_divider + bytes_packettime
                + self.line_separator
            )
        )


def write_export(pdu_data: bytes, packettime: float):
    """
    This method provides the PDU, and additional data in the format as it would be in the logger file.
    This is useful when exporting in real time, since the exporter expects the received data to be of that format.
    :param pdu_data: bytes
    :param packettime: float
    :return: bytes
    """
    line_divider = b"line_divider"
    bytes_packettime = struct.pack("d", packettime)

    return pdu_data + line_divider + bytes_packettime


def _read_u64_le(mv, off: int) -> int:
    return int.from_bytes(mv[off:off + 8], "little", signed=False)


def _write_u64_le(mv, off: int, v: int) -> None:
    mv[off:off + 8] = int(v).to_bytes(8, "little", signed=False)


def _write_u32_le(mv, off: int, v: int) -> None:
    mv[off:off + 4] = int(v).to_bytes(4, "little", signed=False)


def receiving_messages(PORT, start, shm_name, playLoad_max, written_index, n_slot, header_size, slot_hdr, slot_size,
                       TIME_OFF,
                       DATA_OFF, SEQ_OFF, SIZE_OFF, message_len: int = 8192):
    """
    this function is responsible for receiving the dis messages from the network and attach them a packet time.
    the function is intended to run as a separate process
    :SharedMemoryRing: Ring of Slot every message have a slot, every new message is insert into the ring
    :param sock: socket.socket: the socket to receive th DIS messages
    :param header_size: size of the header of the slot
    :param start: float: the time stamp of the start of the logger
    """
    shm = shared_memory.SharedMemory(name=shm_name)
    buf = shm.buf
    print("rec", os.getpid())

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # udp connection
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 128 * 1024 * 1024)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("", PORT))

    try:
        while True:
            try:
                received_data, addr = sock.recvfrom(message_len)
                world_times = datetime.datetime.now().timestamp()
                packet_time = world_times - start
                n = len(received_data)  # be sure that n <= slot_size

                wi = _read_u64_le(buf, written_index)  # choose the slot we want
                slot = wi % n_slot  # if we arrived at the max num of slot we restart from the beginning
                slot_off = header_size + slot * slot_size  # transform the slot num into place into the memory example: slot2 = 4201 into the memory

                _write_u64_le(buf, slot_off + SEQ_OFF, 0)  # that say to the parsing that the slot is not ready

                _write_u32_le(buf, slot_off + SIZE_OFF, n)  # write the size of the message  (n)

                buf[slot_off + TIME_OFF: slot_off + TIME_OFF + 8] = struct.pack("<d",
                                                                                packet_time)  # write the packettime into the slot
                buf[slot_off + DATA_OFF: slot_off + DATA_OFF + n] = received_data  # write the data into the slot

                _write_u64_le(buf, slot_off + SEQ_OFF, wi + 1)  # finished writing able to be read
                _write_u64_le(buf, written_index, wi + 1)  # say to the parsing that a new message had be published
            except WindowsError as e:
                print(f"erro into the try of the while {e}")
                sys.exit()

    except KeyboardInterrupt:
        sock.close()
        shm.close()
        print('receiving process closed')


def parsing_messages(log_queue, shm_name, EXERCISE_ID, written_index, n_slot, header_size, slot_size, slot_hdr, SEQ_OFF,
                     SIZE_OFF,
                     payLoad_max,
                     DATA_OFF, pdus_list, TIME_OFF, CLAIM_INDEX, claim_lock: Lock, lpp: LoggerPduProcessor):
    """
    this function is responsible for parsing the dis message and attach them a packet time.
    the function is intended to run as a separate process
    """
    configure_worker_logger(log_queue)
    log = logging.getLogger("parser")
    shm = shared_memory.SharedMemory(name=shm_name)
    buf = shm.buf
    log.info("enter into parsing")
    global count_total
    print("pars", os.getpid())
    try:
        while True:
            with claim_lock:
                wi = _read_u64_le(buf, written_index)  # num of messages that been published by the receiver
                ci = _read_u64_le(buf,
                                  CLAIM_INDEX)  # shared index between all the process (help to not read the same slot)
                if ci >= wi:  # check if there is a new message
                    idx = None
                else:
                    idx = ci
                    _write_u64_le(buf, CLAIM_INDEX, ci + 1)

            if idx is None:
                time.sleep(0.0005)
                continue
            slot = idx % n_slot  # emplacement of the slot to read into the ring
            slot_off = header_size + slot * slot_size  # address of the slot in bytes

            expected_seq = idx + 1  # only if the idx = 0   because seq = 0 is reserved to the slot that are not ready

            while True:
                seq = _read_u64_le(buf, slot_off + SEQ_OFF)  # save the value of the seq
                if seq == expected_seq:  # in case that the receive end writting break
                    break

                if seq > expected_seq:  # not ready waiting for the receive to finish
                    expected_seq = None
                    break
                time.sleep(0.0001)

            if expected_seq is None:
                print("missed message -----")
                log.info("a message has been dropped")
                continue

            size = int.from_bytes(buf[slot_off + SIZE_OFF:slot_off + SIZE_OFF + 4], "little",
                                  signed=False)  # how many bytes is the data

            if size <= 0 or size > payLoad_max or (DATA_OFF + size) > slot_size:
                print("missed message 2 -----")
                log.info("a message has been dropped")
                continue

            packetTime = struct.unpack("<d", buf[slot_off + TIME_OFF:slot_off + TIME_OFF + 8])[0]
            data_ = bytes(buf[slot_off + DATA_OFF:slot_off + DATA_OFF + size])
            try:
                if data_[2] not in pdus_list:  # only the pdu type we want like entity state event report etc.
                    continue
                received_pdu = LoggerPDU(  # matanb check # MAYBE REMOVE WRITE_EXPORT
                    write_export(data_, packetTime)
                )
            except struct.error as e:
                print(f"Struct exception (shibolet): {e}")
                log.error(f"Struct exception (shibolet): {traceback.format_exc()}")
                continue
            except ValueError as e:
                pass

            if received_pdu.pdu is not None:  # this is temporary until pituah choose a port for dev and a port for prod so we dont have to add a filter off id
                if received_pdu.pdu.exerciseID == EXERCISE_ID:
                    count_total += 1
                    lpp.process(received_pdu)
    except KeyboardInterrupt:
        # print(f"process {os.getpid()} ended")
        print(f"process {os.getpid()} receive total : {count_total} ")
        log.info(f"process {os.getpid()} receive total : {count_total} ")
        shm.close()


def writing_messages(output_file_name: str, experiment_dir: str, logger_dir: str,
                     data_team_dir,
                     q: multiprocessing.Queue, stop_event: threading.Event):
    """
    This method runs in a thread and writes the messages to the lzma file from the writing queue
    :param output_file_name: str
    :param experiment_dir: str
    :param logger_dir: str
    :param data_team_dir: str
    :param q: multiprocessing.Queue: The queue that contains the messages that need to be written
    :param stop_event: threading.Event
    """
    lzma_compressor = lzma.LZMACompressor()
    with DataWriter(output_file_name, experiment_dir, logger_dir, data_team_dir, lzma_compressor) as writer:
        while not (stop_event.is_set() and q.empty()):
            if not q.empty():
                d, packet_time = q.get()
                writer.write(d, packet_time)
        print("writing interrupted ")


def check_loggerfile_name(database_name, logger):
    """
    This method checks if the loggerfile name already exists in the database, if it does, it asks if you want to rename
    the loggerfile.
    """
    old_logger = logger
    logger = logger.removesuffix('.lzma')
    logger_types = ('exp', 'check', 'integration')
    for logger_type in logger_types:
        if logger.lower().startswith(f'{logger_type}'):
            curr_date = datetime.datetime.now()
            logger = f'{logger_type}_{curr_date.day}_{curr_date.month}'
            break
    with sqlConn(database_name) as sql_conn:
        query = f"SELECT TOP 1 LoggerFile FROM Loggers WHERE LoggerFile LIKE '{logger}%' ORDER BY WorldTime DESC"
        exists_df = pd.read_sql_query(query, sql_conn)
    if len(exists_df) != 0:
        logger = exists_df['LoggerFile'][0].removesuffix('.lzma')
        logger_num = re.findall(r'\d+', logger)
        if not logger_num or not logger[-1].isdigit():
            logger = logger + '_2'
        else:
            num_i = logger.rfind(logger_num[-1])
            logger = logger[:num_i] + str(int(logger_num[-1]) + 1)
        if old_logger.removesuffix('.lzma') != logger:
            print(f"loggerfile name {old_logger.removesuffix('.lzma')}.lzma isn't subsequent or it already exits")
            new_logger = input(f'Do you want to name the new logger {logger}.lzma and continue? [y/N]')
            if new_logger != 'y':
                sys.exit()
    else:
        if not logger[-2:] == '_1' and not (datetime.datetime.now().month == 1 and logger[-4:] == '_1_1'):
            logger += '_1'
    logger += '.lzma'
    config_data['logger_file'] = logger
    if old_logger != logger:
        print(f'Logger name changed to {logger}')
        with open('DataExporterConfig.json', 'w') as config:
            json.dump(config_data, config, indent=4)
    return logger


def open_error_log(notepad):
    print("Program ended check the log")
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.wShowWindow = 7  # SW_MINIMIZE (win32con.SW_MINIMIZE = 7)
    if not os.path.exists(notepad):
        notepad = "notepad.exe"  # fallback to Windows built-in notepad
    subprocess.Popen([notepad, "dis-logger.log"])
    print("""
    ============================================================
    PLEASE WAIT FOR THE WINDOW TO CLOSE ITSELF
    ============================================================
    """)


def distribute_locs(num_of_process, locations_per_second):
    """
    Every process will receive the same amount of locs to parse
    """

    result = locations_per_second // num_of_process
    if result == 0:
        return [1 for i in range(num_of_process)]
    else:
        dist_list = [result for p in range(num_of_process)]
        remainder = locations_per_second % num_of_process
        for _ in range(remainder):
            dist_list[_] += 1
        return dist_list


def export_process_func(log_queue, processing_queue, logger_file, tracked_tables, db_name, start_time, export_delay,
                        export_size, new_db):
    """

    function that is need to export the data
    table: str: name of the table that attach to the data
    Data: the data that we want to export
    """
    print("export", os.getpid())
    configure_worker_logger(log_queue)
    log = logging.getLogger("export")
    log.info("enter into export")
    stop_writing_event = threading.Event()
    lse = LoggerSQLExporter(logger_file, tracked_tables, db_name, stop_writing_event, start_time, export_delay,
                            export_size, new_db)
    try:
        while not (processing_queue.empty() and stop_writing_event.is_set()):
            try:
                table, data = processing_queue.get()
                try:
                    lse.export(table, data)
                    # print('exporting')
                except Exception as e:
                    log.error(f"error has occurred while processing a message:{e}")
                    print(f"error  has occurred while processing a message: {e},{traceback.format_exc()}")
            except Exception as e:
                log.error(f"error has occurred while exporting a message:{e}")
                print(f"error  has occurred while exporting a message: {e},{traceback.format_exc()}")
    except KeyboardInterrupt:
        print(f"export process {os.getpid()} stopped")


if __name__ == "__main__":
    freeze_support()  # ensures multiprocessing for exe
    try:
        with open("DataExporterConfig.json", 'r') as f:
            config_data = json.load(f)
    except FileNotFoundError:
        print(r"""
            ERROR: No configuration file
            Please write a configuration file in the base folder by the name "DataExporterConfig.json"
            For examples, see \\files\docs\DataExporter\DataExporterConfig.json
        """)
        sys.exit()

    log_queue = Queue(
        maxsize=50_000)  # it is possible that the queue is to small at the end of the run the notepad will indicate you that the text is too long
    listener = start_log_listener(log_queue, "dis-logger.log", level=logging.DEBUG)

    configure_worker_logger(log_queue)
    log = logging.getLogger("main")

    EXERCISE_ID = config_data["exercise_id"]
    export = config_data["export_to_sql"]
    db_name = config_data["database_name"]
    new_db = config_data["new_database"]
    message_length = config_data["message_length"]
    data_team = config_data["data_team_dir"]
    entity_locations_per_seconds = config_data["entity_locations_per_second"]
    notepad_path = config_data["notepad_path"]
    tracked_tables = config_data["tracked_tables"].replace(' ', '').split(',')
    scenario = config_data["Scenario"]
    export_delay = config_data["export_delay"]
    export_size = config_data["export_size"]
    batch_size = config_data["BatchSize"]
    batch_time = config_data["BatchTime"]
    header_size = config_data["HEADER_SIZE"]
    slot_size = config_data["SLOT_SIZE"]
    n_slot = config_data["N_SLOT"]
    slot_hdr = config_data["SLOT_HDR"]
    PORT = config_data["PORT"]
    pdu_type_list = [1, 2, 3, 21, 33]
    try:
        logger_file = check_loggerfile_name(db_name, config_data["logger_file"])
    except sqlalchemy.exc.InterfaceError or pyodbc.InterfaceError:
        print(f"The database '{db_name}' does not exist in the SQL Server! please check the config")
        sys.exit()
    log.info(f"Log {config_data['logger_file']} started on {datetime.datetime.now()}")
    log.info(
        "This file is generated in case of things going wrong. Aside from this message, I hope it to be empty.")
    """
    :param port: int : port on which dis is transmitted (usually 3000)
    :param timeout: int : Amount of time to wait before giving up
    """
    total_size = header_size + (n_slot * slot_size)
    payLoad_max = slot_size - slot_hdr
    written_index = 0

    SEQ_OFF = 0
    SIZE_OFF = 8
    TIME_OFF = 12
    DATA_OFF = 20
    CLAIM_INDEX = 8

    shm = shared_memory.SharedMemory(create=True,
                                     size=total_size)  # initialization of the ring for the shared memory ring
    shm.buf[written_index:written_index + 8] = (0).to_bytes(8, "little", signed=False)

    buf = shm.buf

    buf[:header_size] = b"\x00" * header_size
    buf[written_index:written_index + 8] = (0).to_bytes(8, "little")
    buf[CLAIM_INDEX:CLAIM_INDEX + 8] = (0).to_bytes(8, "little")

    claim_lock = Lock()

    message_queue = Queue()
    print("main", os.getpid())
    processing_queue = Queue()
    writing_queue = Queue()
    parsing_processes_list = []
    start_time = datetime.datetime.now().timestamp()
    receiving_messages_process = Process(target=receiving_messages,
                                         args=(
                                             PORT, start_time, shm.name, payLoad_max, written_index, n_slot,
                                             header_size,
                                             slot_hdr, slot_size, TIME_OFF, DATA_OFF, SEQ_OFF, SIZE_OFF,
                                             message_length))
    stop_writing_event = threading.Event()
    num_of_parsing_processes = 4
    distribution_list = distribute_locs(num_of_parsing_processes, entity_locations_per_seconds)
    entities = True
    aggregates = True
    for _ in range(num_of_parsing_processes):
        done_processing = multiprocessing.Event()
        lpp = LoggerPduProcessor(processing_queue, distribution_list[_], logger_file, start_time, entities, aggregates)
        parsing_process = Process(target=parsing_messages,
                                  args=(
                                      log_queue, shm.name, EXERCISE_ID, written_index, n_slot, header_size, slot_size,
                                      slot_hdr,
                                      SEQ_OFF, SIZE_OFF,
                                      payLoad_max, DATA_OFF, pdu_type_list, TIME_OFF, CLAIM_INDEX, claim_lock, lpp))
        parsing_processes_list.append(parsing_process)

    receiving_messages_process.start()
    for process in parsing_processes_list:
        process.start()
    writing_thread = threading.Thread(target=writing_messages, args=(
        logger_file, db_name, 'logs', data_team, writing_queue, stop_writing_event))
    writing_thread.start()

    export_processes_list = []
    num_of_exporting_processes = 1
    if export:
        start_logger_data = {
            "LoggerFile": logger_file,
            "Scenario": None if scenario == "" else scenario,
            "WorldTime": datetime.datetime.fromtimestamp(start_time),
            "ExerciseId": EXERCISE_ID
        }

        lse = LoggerSQLExporter(logger_file, tracked_tables, db_name, stop_writing_event, start_time, export_delay,
                                export_size, new_db=new_db)
        lse.export("Loggers", start_logger_data)
        for _ in range(num_of_exporting_processes):
            export_process = Process(target=export_process_func,
                                     args=(
                                         log_queue, processing_queue, logger_file, tracked_tables, db_name, start_time,
                                         export_delay,
                                         export_size, new_db))
            export_processes_list.append(export_process)
        for exprocess in export_processes_list:
            exprocess.start()
        while not stop_writing_event.is_set():
            try:
                last = time.time()
                time.sleep(1.0)
            except KeyboardInterrupt:
                print("""
                ============================================================
                PLEASE WAIT FOR THE WINDOW TO CLOSE ITSELF
                ============================================================
                """)
                stop_writing_event.set()
                for p in parsing_processes_list:
                    p.terminate()
                while not message_queue.empty():
                    time.sleep(1)
                    print("waiting for message queue to be emptied")
                    print(message_queue.qsize())
                print(f'processing queue size,{processing_queue.qsize()}')
                for pars_process in parsing_processes_list:
                    if pars_process.is_alive():
                        pars_process.terminate()
                while not processing_queue.empty():
                    time.sleep(1)
                    print("waiting for processing queue to be empty")
                print(f'after,{processing_queue.qsize()}')
                for p2 in export_processes_list:
                    p2.terminate()
                for exp_process in export_processes_list:
                    if exp_process.is_alive():
                        exp_process.terminate()
                while not log_queue.empty():
                    time.sleep(1)
                listener.stop()

        print('exit while', processing_queue.qsize())
        writing_thread.join()
        open_error_log(notepad_path)
    else:
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            stop_writing_event.set()
            writing_thread.join()
            open_error_log(notepad_path)