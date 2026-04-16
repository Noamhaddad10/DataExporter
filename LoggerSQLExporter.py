import sys

sys.setrecursionlimit(sys.getrecursionlimit() * 5)
import datetime
import multiprocessing
import json
import os
import logging
import threading
import time
import urllib.parse
import sqlalchemy

log = logging.getLogger("LoggerSQLExporter")


def sql_engine(db: str):
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost\\SQLEXPRESS;"
        f"DATABASE={db};"
        "Trusted_Connection=yes;"
    )
    params = urllib.parse.quote_plus(conn_str)
    engine = sqlalchemy.create_engine(
        "mssql+pyodbc:///?odbc_connect=%s" % params,
        pool_pre_ping=True,
        pool_size=20,
        max_overflow=0,
        fast_executemany=True
    )
    return engine


class Exporter:
    """
    An instance of this class is created for each table to which data is exported, eg EntityLocations,
    Entities, FirePDU, etc.
    This enables dumping data to SQL through numerous threads.
    This is useful since one must wait for SQL to return that the entry of data was a success, but if it's split off
    to another thread, then it doesn't affect the mainloop.
    """

    def __init__(self, stop_event: multiprocessing.Event, table_name: str, sql_meta: sqlalchemy.MetaData,
                 sql_engine: sqlalchemy.engine, tracked_tables: list, start_time, export_delay, export_size, data=None):
        """
        :param table_name: str : The name of the target table
        :param sql_meta: sqlalchemy.MetaData
        :param sql_engine: sqlalchemy.engine
        """
        data = data if data is not None else []
        self.table_name = table_name
        self.count = 0
        self.count_export = 0
        self.count_insert = 0
        self.table = sqlalchemy.Table(table_name, sql_meta, autoload_with=sql_engine)
        self.sql_engine = sql_engine
        self.tracked_tables = tracked_tables
        # These are the tables that print their status into stdout
        # I have selected these 2 since they are the most active, and most likely to delay the end of the program
        self.logger_table = "Loggers"
        self.data = data
        self.stop_event = stop_event
        self.start_time = start_time
        self.export_delay = export_delay
        self.export_size = export_size
        self.lock = threading.Lock()
        if self.table_name == self.logger_table:
            self.insert(data)
        else:
            threading.Thread(target=self.export, args=()).start()

    def add_data(self, data_to_add: list):
        """
        Adds rows to the internal variable which holds data to export to the database
        :param data_to_add: list[dict]
        :return: None
        """
        with self.lock:
            self.data += data_to_add

    def export(self):
        """
        Exports data to the target table.
        This is targeted by a thread, and should not really be called by you
        :return: None
        """
        with self.lock:
            data = self.data
            self.data = []
        if len(data) == 0:
            if self.table_name in self.tracked_tables:
                log.debug("No data in %s", self.table_name)
            if not self.stop_event.is_set():
                # Only keep exporting data while the main thread is still alive
                # If the main thread has finished, then no more data will reach here, and so this thread can end
                threading.Timer(1, self.export).start()
        elif self.table_name == "EntityLocations":
            if len(data) < self.export_size and self.count > 0:
                self.count += 1
                with self.lock:
                    self.data.extend(data)
                threading.Timer(self.export_delay, self.export).start()
                time.sleep(0.05)
                return
            else:
                self.count = 0
                ExportTime = datetime.datetime.now()
                for i in range(len(data)):
                    data[i]["ExportTimeToDb"] = ExportTime.timestamp() - self.start_time
                self.count_export = self.count_export + len(data)
                log.debug("count_export: %d", self.count_export)
                self.insert(data)
                threading.Thread(target=self.export, args=()).start()
        else:
            ExportTime = datetime.datetime.now()
            for i in range(len(data)):
                data[i]["ExportTimeToDb"] = ExportTime.timestamp() - self.start_time
                # if self.table_name == "Aggregates":
                #     if data[i]["EntityId"] == '0:0:0':
                #         print(data[i])
            self.insert(data)
            threading.Thread(target=self.export, args=()).start()

    def insert(self, data):
        # FIX BUG3: correct MAX_TRIES logic -- no infinite loop, no manual rollback,
        # no print(range(10000)) loops.
        MAX_TRIES = 5
        attempt = 0
        while attempt < MAX_TRIES:
            attempt += 1
            try:
                with self.sql_engine.begin() as connection:
                    if self.table_name in self.tracked_tables:
                        start = datetime.datetime.now()
                        log.debug("Exporting: %s, start=%s, size=%d, pid=%d", self.table_name, start, len(data), os.getpid())
                    if self.table_name == "EntityLocations":
                        self.count_insert = self.count_insert + len(data)
                    connection.execute(self.table.insert(), data)
                    if self.table_name in self.tracked_tables:
                        log.debug("Done: %s, pid=%d, time=%s", self.table_name, os.getpid(), datetime.datetime.now() - start)
                    return  # success -- exit the loop

            except Exception as e:
                err_str = str(e)
                # Duplicate key on Entities: expected (unique index), ignore silently
                if 'Entities_UIX' in err_str or 'Duplicate key' in err_str:
                    return
                # SQL Server deadlock: retry with back-off
                if 'deadlocked' in err_str.lower() or 'rolled back' in err_str.lower():
                    log.warning(
                        "Deadlock detected (attempt %d/%d) table=%s -- retry in 50ms",
                        attempt, MAX_TRIES, self.table_name
                    )
                    if attempt >= MAX_TRIES:
                        log.error("WE LOST A MESSAGE -- max retries exceeded for table=%s", self.table_name, exc_info=True)
                        return
                    time.sleep(0.05)
                else:
                    # Non-retryable error: log and give up
                    log.error("Insert error table=%s: %s", self.table_name, e, exc_info=True)
                    return


class LoggerSQLExporter:
    """
    This class manages the individual Exporter instances, and received data from other places, such as the Logger, or
    a file.
    Each base message has its own export method, in order to make the format of the data going in to SQL
    Each Event Report is handled by a single method, that calls to EventReportInterpreter, and uses the encoder
    to understand.

    Some data is stored persistently here, to be disseminated to the messages.
    An example of this is the ExporterMarkingText, which is stored in a dictionary where the keys are the __str__() of
    the EntityID (or whatever is most amenable)
    """

    def __init__(self, logger_file: str, tracked_tables: list, export_db: str, stop_event: multiprocessing.Event,
                 start_time2, export_delay, export_size,
                 new_db: bool = False):
        """
        :param logger_file: str
        :param export_db: str
        :param new_db: bool
        """
        if new_db:
            log.warning(
                "new_db=True is no longer supported (create_json/create_all_tables removed). "
                "Create tables manually via SQL scripts before starting the pipeline."
            )

        self.pdu_encoder = None

        self.sql_engine = sql_engine(export_db)
        self.sql_meta = sqlalchemy.MetaData(schema="dis")
        self.stop_event = stop_event
        self.logger_file = logger_file
        self.export_time = datetime.datetime.now()
        self.start_time2 = start_time2
        self.export_delay = export_delay
        self.export_size = export_size
        # self.exercise_id = exercise_id

        # Indicates which table connections to make on program start up. Currently only the base tables.
        self.starter_sql_tables = ["EntityLocations", "Entities", "FirePdu",
                                   "DetonationPdu"]  # need to check if we add AggregateState

        self.loggers_table = "Loggers"

        self.tracked_tables = tracked_tables
        # Stores the mapping from EntityID to MarkingText
        self.exporter_marking_text = {}

        # This dict of the Exporters to which data is passed to be sent to tables in a multithreading manner
        self.exporters = {
            name: Exporter(self.stop_event, name, self.sql_meta, self.sql_engine, self.tracked_tables, start_time2,
                           export_delay, export_size) for
            name in self.starter_sql_tables}

        # self.read_encoder()

    def export(self, table: str, d: list[dict]):
        """
        Accepts the name of the target table, and the data to insert as a list of dicts. Then passes that data on to
        the relevant Exporter, and if there is no such Exporter, creates one.
        :param table: str
        :param d: list[dict]
        :return: None
        """
        if table == self.loggers_table:
            Exporter(self.stop_event, table, sqlalchemy.MetaData(schema="dbo"), self.sql_engine, self.tracked_tables,
                     self.start_time2, self.export_delay, self.export_size, d)
            return
        if table not in self.exporters:
            self.exporters[table] = Exporter(self.stop_event, table, self.sql_meta, self.sql_engine,
                                             self.tracked_tables, self.start_time2, self.export_delay, self.export_size)
        self.exporters[table].add_data(d)

    def insert_sync(self, table: str, data: list) -> bool:
        """
        Synchronous SQL insert -- bypasses the add_data() timer thread.
        Used by kafka_consumer to guarantee: SQL insert committed BEFORE Kafka offset commit.
        (Fix BUG 3: Kafka offset was committed before SQL insert)

        Returns True if the insert succeeded, False otherwise.
        On failure, the caller does NOT commit the offset -> reprocessed on restart.
        """
        if table == self.loggers_table:
            try:
                Exporter(
                    self.stop_event, table, sqlalchemy.MetaData(schema="dbo"),
                    self.sql_engine, self.tracked_tables,
                    self.start_time2, self.export_delay, self.export_size, data
                )
                return True
            except Exception as exc:
                log.error("insert_sync Loggers failed: %s", exc, exc_info=True)
                return False

        if table not in self.exporters:
            self.exporters[table] = Exporter(
                self.stop_event, table, self.sql_meta, self.sql_engine,
                self.tracked_tables, self.start_time2, self.export_delay, self.export_size
            )

        # Stamp ExportTimeToDb (same as what Exporter.export() does)
        ts = datetime.datetime.now().timestamp() - self.start_time2
        rows = [dict(row, ExportTimeToDb=ts) for row in data]

        try:
            self.exporters[table].insert(rows)
            return True
        except Exception as exc:
            log.error("insert_sync table=%s failed: %s", table, exc, exc_info=True)
            return False


# def load_file_data(logger_file: str, db_name: str, exercise_id: int, new_db=False, debug=False): #TODO update this function
#     """
#     The purpose of this function is to export the given loggerfile of data to SQL.
#     I do not anticipate it being used much, but occasionally we do have to export data to SQL again for some reason or
#     other.
#     You should be warned, using this is intense on the EntityState tables. There is a huge amount of data to enter, and
#     this function inserts it all as fast as humanly possible. These tables, being the largest, are often out of
#     commission for the duration.
#
#     :param logger_file: str : Name of the logger file to export
#     :param db_name: str : Name of the target database
#     :param exercise_id: int : Exercise ID of experiment to export
#     :param new_db: bool : Whether or not this is a new database (empty) or not
#     :param debug: bool : Debug mode, more logging detail
#     :return: None
#     """
#     if debug:
#         errors = 0
#         separator_errors = 0
#         struct_unpack_errors = 0
#         pdu_unpack_error = 0
#
#     with lzma.open(f"logs/{logger_file}", 'r') as f:
#         raw_data = f.read().split(b"line_separator")
#
#         if debug:
#             data = []
#         logger_sql_exporter = LoggerSQLExporter(logger_file, db_name, exercise_id, new_db=new_db)
#
#         total = len(raw_data)
#         print(f"Start time: {datetime.datetime.now()}")
#         print(f"Total packets: {len(raw_data):,}")
#         for i, line in enumerate(raw_data):
#             # Give progress updates when exporting. It's nice to see how far along we are.
#             if i % 100_000 == 0:
#                 print(f"{i:,}")
#
#             if line.count(b"line_divider") == 2:
#                 try:
#                     logger_pdu = LoggerPDU(line)
#
#                     if debug:
#                         data.append(logger_pdu)
#
#                     logger_sql_exporter.export(logger_pdu)
#
#                 except ValueError:
#                     if debug:
#                         errors += 1
#                         separator_errors += 1
#                     continue
#                 except BytesWarning:
#                     if debug:
#                         errors += 1
#                         pdu_unpack_error += 1
#                     continue
#                 except struct.error:
#                     if debug:
#                         errors += 1
#                         struct_unpack_errors += 1
#                     continue
#                 except KeyError:  #  Bad fix, there are event reports that are not wanted, so find a way to deal with them
#                     continue
#             else:
#                 if debug:
#                     errors += 1
#                     separator_errors += 1
#                 continue
#
#         print("Loaded file")
#     if debug:
#         logging.info(f"Total pdus:              {total}")
#         logging.info(f"Total errors:            {errors},               {100 * errors / total}%")
#         logging.info(f"Seperator errors:        {separator_errors},     {100 * separator_errors / total}%")
#         logging.info(f"Unpacking time errors:   {struct_unpack_errors}, {100 * struct_unpack_errors / total}%")
#         logging.info(f"Unpacking pdu errors:    {pdu_unpack_error},     {100 * pdu_unpack_error / total}%")


if __name__ == "__main__":
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

    if config_data["logger_file"][-5:] != ".lzma":
        config_data["logger_file"] += ".lzma"

    # exercise_id = config_data["exercise_id"]
    logger_file = config_data["logger_file"]
    db_name = config_data["database_name"]
    new_db = config_data["new_database"]

    start_time = time.perf_counter()
    # load_file_data(logger_file, db_name, exercise_id, new_db=new_db)
    end_time = time.perf_counter()
    print(f"Execution time: {datetime.timedelta(seconds=(end_time - start_time))}")
    print("""
    ============================================================
    PLEASE WAIT FOR THE WINDOW TO CLOSE ITSELF
    ============================================================
    """)
