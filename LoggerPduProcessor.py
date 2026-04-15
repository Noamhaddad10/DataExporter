import sys
sys.setrecursionlimit(sys.getrecursionlimit() * 5)
import datetime
import logging
import json
import multiprocessing
import os
import struct
import copy
import math
import time
import opendis
from opendis import dis7
from opendis.PduFactory import createPdu
count_parsing = 0
nans = ["GeoLocationX", "GeoLocationY", "GeoLocationZ", "GeoVelocityX", "GeoVelocityY", "GeoVelocityZ"]


class LoggerPDU:
    """
    Contains any given PDU in the PDU field, and PacketTime data
    """

    def __init__(self, logger_line):
        """
        param logger_line: bytes : a line of bytes from the loggerfile
        These are the of the format : pdu_data and PacketTime
        """
        self.pdu = None
        self.packet_time = 0.0

        self.interpret_logger_line(logger_line)

    @classmethod
    def from_parts(cls, pdu_data: bytes, packet_time: float) -> "LoggerPDU":
        """
        Construit un LoggerPDU directement depuis les bytes bruts et le packet_time,
        SANS passer par la serialisation b'line_divider'.

        Fix BUG 2 : elimine tout risque de collision si les bytes du PDU
        contiennent accidentellement la sequence b'line_divider' (0x6c696e655f646976696465...).
        Utilise par kafka_consumer.py a la place du constructeur standard.

        :param pdu_data:    bytes bruts du PDU DIS (sans header Kafka)
        :param packet_time: temps relatif au demarrage du pipeline (float)
        """
        obj = cls.__new__(cls)
        obj.pdu = createPdu(pdu_data)
        obj.packet_time = packet_time
        return obj

    def interpret_logger_line(self, logger_line: bytes):
        """
        Unpacks a line received from the logger into the object
        :param logger_line: bytes : a line of bytes from the loggerfile
        These are the of the format : pdu_data, PacketTime
        """
        if logger_line.count(b"line_divider") == 1:
            split_line = logger_line.split(b"line_divider")
        else:
            raise ValueError(
                f"Given line of data does not contain enough separators. Perhaps some data is missing: {logger_line}")
        self.pdu = createPdu(split_line[0])
        self.packet_time = struct.unpack("d", split_line[1])[0]  # struct.unpack always returns a tuple


class EventReportInterpreter:
    """
    Interprets an Event Report from a collection of bytes, into something useful based off the format
    found in the PduEncoder.json
    """

    def __init__(self, pdu: LoggerPDU, pdu_encoder: dict):
        """
        :param pdu: LoggerPDU
        :param pdu_encoder: dict
        """
        self.logger_pdu = pdu
        self.event_num = self.logger_pdu.pdu.eventType
        self.event_name = pdu_encoder[str(self.event_num)]["event_name"]

        self.pdu_encoder = pdu_encoder

        self.variable_data = {}
        self.fixed_data = {}
        self.base_data = {}
        self.nans = nans

        self.interpret_pdu()

    def __str__(self):
        return f"{self.variable_data}\n{self.fixed_data}"

    def interpret_pdu(self):
        """
        Interprets the Event Report into something understandable, rather than the collection of obscure bytes
        :return: None
        """
        for var_data, data_name in zip(self.logger_pdu.pdu._datums.variableDatumRecords,
                                       self.pdu_encoder[str(self.event_num)]["VariableData"].keys()):
            decoded_var_data = var_data.variableData
            decoded_var_data = bytes(decoded_var_data)
            decoded_var_data = decoded_var_data.decode("utf-8").rstrip('\x00')
            self.variable_data[data_name] = decoded_var_data

        for fixed_data, data_name, data_type in zip(self.logger_pdu.pdu._datums.fixedDatumRecords,
                                                    self.pdu_encoder[str(self.event_num)]["FixedData"].keys(),
                                                    self.pdu_encoder[str(self.event_num)]["FixedData"].values()):
            try:
                if data_type == "Float64":
                    datum_as_hex = hex(fixed_data.fixedDatumValue)[2:]
                    if len(datum_as_hex) != 8:
                        zeros_to_add = '0' * (8 - len(datum_as_hex))
                        datum_as_hex = zeros_to_add + datum_as_hex
                    datum_as_bytes = bytes.fromhex(datum_as_hex)
                    datum_as_float = struct.unpack("!f", datum_as_bytes)[0]
                    if math.isnan(datum_as_float):
                        datum_as_float = None

                    self.fixed_data[data_name] = datum_as_float

                elif data_type == "Int32":
                    if math.isnan(fixed_data.fixedDatumValue):
                        self.fixed_data[data_name] = None
                    else:
                        datum_as_bytes = struct.pack('>I', fixed_data.fixedDatumValue)
                        self.fixed_data[data_name] = struct.unpack('>i', datum_as_bytes)[0]

                elif data_type == "UInt32":
                    if math.isnan(fixed_data.fixedDatumValue):
                        self.fixed_data[data_name] = None
                    else:
                        self.fixed_data[data_name] = fixed_data.fixedDatumValue
            except Exception as e:
                self.fixed_data[data_name] = None
                print(f'error dealing with float64 {self.event_name} ,{data_name}')
        self._get_base_data()

    def _get_base_data(self):
        """
        There is an amount of data that exists for every Event Report. This method makes that data.
        :return: None
        """
        self.base_data = self.base_data | {
            "PacketTime": self.logger_pdu.packet_time,
        }

        # Loggerfile, Export time, and exercise id are dealt by the LSE


class LoggerPduProcessor:
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

    def __init__(self, queue: multiprocessing.Queue, n_of_entity_location_per_sec: float, logger_file: str,
                 start_time: float, process_entities: bool, process_aggregates):

        self.pdu_encoder = None

        self.logger_file = logger_file
        self.queue = queue
        self.exporter_marking_text = {}
        self.entity_locs_cache = {}
        self.to_process_entities = process_entities
        if self.to_process_entities:
            self.entities_cache = {}
        self.agr_locs_cache = {}
        self.to_process_aggregates = process_aggregates
        if self.to_process_aggregates:
            self.agr_cache = {}
        self.entity_state_er_cache = {}
        self.n = n_of_entity_location_per_sec
        self.nanable_data = nans
        self.play_stop_situation_dict = {-1: "pre", 0: "play", 1: "stop"}
        #self.read_encoder()
        self.start_time = start_time

    def replace_nans(self, d: dict):
        for key in d.keys():
            if key in self.nanable_data:
                if math.isnan(d[key]):
                    d[key] = None
        return d

    def process(self, logger_pdu: LoggerPDU):
        """
        Accepts a LoggerPDU, and passes the data to the methods for dealing with each individual case.
        All EventReports are sent to a single method, and the base messages are given their own methods.
        :param logger_pdu: LoggerPDU
        :return: None
        """
        pdu_type = type(logger_pdu.pdu)
        # if pdu_type == opendis.dis7.EventReportPdu:
        #     if str(logger_pdu.pdu.eventType) not in self.pdu_encoder:
        #         # Only the Event Reports mentioned in the PduEncoder
        #         return
        #     event_report = EventReportInterpreter(logger_pdu, self.pdu_encoder)
        #     # if event_report.event_name == "PlayStopAnalysis":
        #     #     play_or_stop = event_report.fixed_data["action"]
        #     #     if play_or_stop == 0:
        #     #         # Yes, 0 indicates the start of the experiment in this EventReport. Yay enums.
        #     #         self.play_stop_situation.value = 0
        #     #     elif play_or_stop == 1:
        #     #         self.play_stop_situation.value = 1
        #     #     else:
        #     #         logging.error(f"INCORRECT VALUE {play_or_stop} RECEIVED IN PlayStopAnalysis. SHOULD BE 0/1")
        #     self._process_event_report(event_report)
        # else:
        if pdu_type == opendis.dis7.EntityStatePdu:
            self._process_entity_state(logger_pdu)
        elif pdu_type == opendis.dis7.FirePdu:
            self._process_fire_pdu(logger_pdu)
        elif pdu_type == opendis.dis7.DetonationPdu:
            self._process_detonation_pdu(logger_pdu)
        elif pdu_type == opendis.dis7.AggregateStatePdu:
            self._process_aggregateState_pdu(logger_pdu)

    def read_encoder(self):
        """
        Loads the PduEncoder into the class for interpreting the EventReports
        :return: None
        """
        encoder_subdir = max(os.listdir("encoders/"))
        with open(f"encoders/{encoder_subdir}/PduEncoder.json", 'r') as f:
            encoder = json.load(f)

        self.pdu_encoder = encoder

    def _get_exporter_marking_text(self, entityid: str):
        try:
            return self.exporter_marking_text[entityid]
        except KeyError:
            return None

    # def _process_event_report(self, event_report: EventReportInterpreter):
    #     """
    #     Takes an event report, creates the base data that is consistent across all EventReports, merges that data
    #     together with the data gathered from the EventReport field, and sends it to the relevant
    #     Exporter for exporting to SQL
    #     :param event_report: EventReportInterpreter
    #     :return: None
    #     """
    #
    #     consistent_base_data = {
    #         "LoggerFile": self.logger_file
    #         # "PlayStop": self.play_stop_situation_dict[self.play_stop_situation.value]
    #     }
    #     data_to_insert = event_report.fixed_data | event_report.variable_data | event_report.base_data | \
    #                      consistent_base_data
    #     self.queue.put((event_report.event_name, [data_to_insert]))

    def _process_entity_state(self, logger_pdu: LoggerPDU):
        """
        This method calls numerous other methods in order to export EntityState to the SQL sub tables.
        First off the base data required by all EntityState tables is created, and is then passed to the
        other methods for incorporation
        :param logger_pdu: LoggerPDU
        :return: None
        """
        # Set ExporterMarkingText
        # TODO not a good fix
        chars = logger_pdu.pdu.marking.characters
        marking_text = ""
        for i in range(len(chars)):
            if chars[i] == 0:
                break
            marking_text += chr(chars[i])
        # try:
        #     marking_text = "".join(map(chr, logger_pdu.pdu.marking.characters))
        # except ValueError as e:
        #     marking_text = "none"
        # self.exporter_marking_text[logger_pdu.pdu.entityID.__str__()] = marking_text.strip("\x00")
        base_data = {
            "EntityId": f"{logger_pdu.pdu.entityID.siteID}:{logger_pdu.pdu.entityID.applicationID}:{logger_pdu.pdu.entityID.entityID}",
            "PacketTime": logger_pdu.packet_time,
            "ExportTimeToDb": float(0),
            "LoggerFile": self.logger_file,
            "ExporterMarkingText": marking_text
        }
        base_data = self.replace_nans(base_data)
        # Locations
        self._entity_locs(logger_pdu, base_data)
        # Entities
        if self.to_process_entities:
            self._entities(logger_pdu, base_data)

    def _entity_locs(self, logger_pdu: LoggerPDU, base_data: dict):
        global count_parsing
        """
        Creates the data for the EntityLocations table
        :param logger_pdu: LoggerPDU
        :param base_data: dict
        :return: None
        """
        entity_locs = base_data | {
            "GeoLocationX": logger_pdu.pdu.entityLocation.x,
            "GeoLocationY": logger_pdu.pdu.entityLocation.y,
            "GeoLocationZ": logger_pdu.pdu.entityLocation.z,

            "GeoVelocityX": logger_pdu.pdu.entityLinearVelocity.x,
            "GeoVelocityY": logger_pdu.pdu.entityLinearVelocity.y,
            "GeoVelocityZ": logger_pdu.pdu.entityLinearVelocity.z,

            "Psi": logger_pdu.pdu.entityOrientation.psi,
            "Theta": logger_pdu.pdu.entityOrientation.theta,
            "Phi": logger_pdu.pdu.entityOrientation.phi
        }
        entity_locs = self.replace_nans(entity_locs)

        e_id = f"{logger_pdu.pdu.entityID.siteID}:{logger_pdu.pdu.entityID.applicationID}:{logger_pdu.pdu.entityID.entityID}"
        seconds_partition = math.floor(self.n * entity_locs["PacketTime"])
        if e_id in self.entity_locs_cache:
            if seconds_partition > self.entity_locs_cache[e_id]:
                self.entity_locs_cache[e_id] = seconds_partition
                self.queue.put(("EntityLocations", [entity_locs]))
                count_parsing += 1
        else:
            self.entity_locs_cache[e_id] = seconds_partition
            self.queue.put(("EntityLocations", [entity_locs]))

    def _entities(self, logger_pdu: LoggerPDU, base_data: dict):
        """
        Creates the data for the Entities table.
        2 rows are inserted for each EntityStatePdu, MarkingText, and EntityType.
        These are created individually, before being sent together to the relevant Exporter.
        :param logger_pdu: LoggerPDU
        :param base_data: dict
        :return: None
        """
        # MarkingText
        # EntityType
        entity_type = logger_pdu.pdu.entityType
        entity_texts_type = base_data | {
            "EntityType": f"{entity_type.entityKind}:{entity_type.domain}:{entity_type.country}:{entity_type.category}:{entity_type.subcategory}:{entity_type.specific}:{entity_type.extra}",
            "ForceId": logger_pdu.pdu.forceId
        }

        overall_dicts = [entity_texts_type]
        s_id = overall_dicts[0]["EntityId"]
        hashed_data = str([{k: d[k] for k in (d.keys() ^ {"PacketTime"})} for d in overall_dicts])
        if s_id in self.entities_cache:
            if hashed_data != self.entities_cache[s_id]:
                self.entities_cache[s_id] = hashed_data
                self.queue.put(("Entities", overall_dicts))

        else:
            self.entities_cache[s_id] = hashed_data
            self.queue.put(("Entities", overall_dicts))

        # self._batch_dicts("EntityStateTexts", overall_dicts)

    def _process_fire_pdu(self, logger_pdu: LoggerPDU):
        """
        Creates data for the FirePDU table
        :param logger_pdu: LoggerPDU
        :return: None
        """
        fire_pdu = {
            "EventId": f"{logger_pdu.pdu.eventID.simulationAddress.site}:{logger_pdu.pdu.eventID.simulationAddress.application}:{logger_pdu.pdu.eventID.eventNumber}",
            "AttackerId": f"{logger_pdu.pdu.firingEntityID.siteID}:{logger_pdu.pdu.firingEntityID.applicationID}:{logger_pdu.pdu.firingEntityID.entityID}",
            "TargetId": f"{logger_pdu.pdu.targetEntityID.siteID}:{logger_pdu.pdu.targetEntityID.applicationID}:{logger_pdu.pdu.targetEntityID.entityID}",
            "MunitionId": f"{logger_pdu.pdu.munitionExpendableID.siteID}:{logger_pdu.pdu.munitionExpendableID.applicationID}:{logger_pdu.pdu.munitionExpendableID.entityID}",
            "GeoLocationX": logger_pdu.pdu.location.x,
            "GeoLocationY": logger_pdu.pdu.location.y,
            "GeoLocationZ": logger_pdu.pdu.location.z,
            "Range": logger_pdu.pdu.range,
            "Quantity": logger_pdu.pdu.descriptor.quantity,
            "Rate": logger_pdu.pdu.descriptor.rate,
            "FuseType": logger_pdu.pdu.descriptor.fuse,
            "WarheadType ": logger_pdu.pdu.descriptor.warhead,
            # "MunitionType": logger_pdu.pdu.descriptor.munitionType,
            "LoggerFile": self.logger_file,
            "WorldTime": datetime.datetime.fromtimestamp(logger_pdu.packet_time + self.start_time),
            "PacketTime": logger_pdu.packet_time
        }
        fire_pdu = self.replace_nans(fire_pdu)

        self.queue.put(("FirePdu", [fire_pdu]))

    def _process_detonation_pdu(self, logger_pdu: LoggerPDU):
        """
        Creates data for the DetonationPDU table
        :param logger_pdu: LoggerPDU
        :return: None
        """
        munition_type = logger_pdu.pdu.descriptor.munitionType
        detonation_pdu = {
            "AttackerId": f"{logger_pdu.pdu.firingEntityID.siteID}:{logger_pdu.pdu.firingEntityID.applicationID}:{logger_pdu.pdu.firingEntityID.entityID}",
            "MunitionType":
                f"{munition_type.entityKind}:{munition_type.domain}:{munition_type.country}:{munition_type.category}:{munition_type.subcategory}:{munition_type.specific}:{munition_type.extra}",
            "TargetId": f"{logger_pdu.pdu.targetEntityID.siteID}:{logger_pdu.pdu.targetEntityID.applicationID}:{logger_pdu.pdu.targetEntityID.entityID}",
            "MunitionId": f"{logger_pdu.pdu.explodingEntityID.siteID}:{logger_pdu.pdu.explodingEntityID.applicationID}:{logger_pdu.pdu.explodingEntityID.entityID}",
            "EventId": f"{logger_pdu.pdu.eventID.simulationAddress.site}:{logger_pdu.pdu.eventID.simulationAddress.application}:{logger_pdu.pdu.eventID.eventNumber}",
            "GeoVelocityX": logger_pdu.pdu.velocity.x,
            "GeoVelocityY": logger_pdu.pdu.velocity.y,
            "GeoVelocityZ": logger_pdu.pdu.velocity.z,
            "GeoLocationX": logger_pdu.pdu.location.x,
            "GeoLocationY": logger_pdu.pdu.location.y,
            "GeoLocationZ": logger_pdu.pdu.location.z,
            "EntityLocationX": logger_pdu.pdu.locationInEntityCoordinates.x,
            "EntityLocationY": logger_pdu.pdu.locationInEntityCoordinates.y,
            "EntityLocationZ": logger_pdu.pdu.locationInEntityCoordinates.z,
            "Quantity": logger_pdu.pdu.descriptor.quantity,
            "Rate": logger_pdu.pdu.descriptor.rate,
            "FuseType": logger_pdu.pdu.descriptor.fuse,
            "WarheadType ": logger_pdu.pdu.descriptor.warhead,
            "Result": logger_pdu.pdu.detonationResult,
            "PacketTime": logger_pdu.packet_time,
            "ExportTimeToDb": float(0),
            "LoggerFile": self.logger_file,
            "WorldTime": datetime.datetime.fromtimestamp(logger_pdu.packet_time + self.start_time)
        }
        detonation_pdu = self.replace_nans(detonation_pdu)
        self.queue.put(("DetonationPdu", [detonation_pdu]))

    def _process_aggregateState_pdu(self, logger_pdu: LoggerPDU):
        """
        This method calls numerous other methods in order to export AggregateState to the SQL sub tables.
        First off the base data required by all EntityState tables is created, and is then passed to the
        other methods for incorporation
        :param logger_pdu: LoggerPDU
        :return: None
        """

        base_data = {
            "PacketTime": logger_pdu.packet_time,
            "ExportTimeToDb": float(0),
            "LoggerFile": self.logger_file
        }
        base_data = self.replace_nans(base_data)
        # Locations
        self._aggregate_locs(logger_pdu, base_data)
        # Aggregates
        if self.to_process_aggregates:
            self._aggregates(logger_pdu, base_data)

    def _aggregates(self, logger_pdu: LoggerPDU, base_data: dict):

        aggregate_type = logger_pdu.pdu.aggregateType
        # try:
        #     marking_text = "".join(map(chr, logger_pdu.pdu.aggregateMarking.characters))
        # except ValueError as e:
        chars = logger_pdu.pdu.aggregateMarking.characters
        marking_text = ""
        for i in range(len(chars)):
            if chars[i] == 0:
                break
            marking_text += chr(chars[i])
        # marking_text = marking_text.strip("\x00")
        Num_Entity_pluga = int(logger_pdu.pdu.numberOfEntityIDs)
        aggregate_pdu = base_data | {
            "AggregateID": str(
                f"{logger_pdu.pdu.aggregateID.simulationAddress.site}:{logger_pdu.pdu.aggregateID.simulationAddress.application}:{logger_pdu.pdu.aggregateID.aggregateID}"),
            "AggregateType": str(
                f"{aggregate_type.aggregateKind}:{aggregate_type.domain}:{aggregate_type.country}:{aggregate_type.category}:{aggregate_type.subcategory}:{aggregate_type.specificInfo}:{aggregate_type.extra}"),
            "ForceId": int(logger_pdu.pdu.forceID),
            "NumEntities": int(logger_pdu.pdu.numberOfEntityIDs),
            # "AggregateState": int(logger_pdu.pdu.aggregateState),
            "AggregateFormation": int(logger_pdu.pdu.formation),
            "markingText": str(marking_text),
            "NumMarking": str(logger_pdu.pdu.aggregateMarking.characters)
        }
        if Num_Entity_pluga > 0:
            for i in range(Num_Entity_pluga):
                Entity_id = logger_pdu.pdu.entityIDs[i]
                entity_aggregate_pdu = base_data | aggregate_pdu | {
                    "EntityId": f"{Entity_id.siteID}:{Entity_id.applicationID}:{Entity_id.entityID}",
                }
                overall_dicts = [entity_aggregate_pdu]
                s_id = overall_dicts[0]["EntityId"]

                if s_id != "0:0:0":
                    hashed_data = str(
                        [{k: d[k] for k in (d.keys() ^ {"PacketTime", "ExportTimeToDb"})} for d in overall_dicts])
                    if s_id in self.agr_cache:
                        if hashed_data != self.agr_cache[s_id]:
                            self.agr_cache[s_id] = hashed_data
                            self.queue.put(("Aggregates", overall_dicts))
                    else:
                        self.agr_cache[s_id] = hashed_data
                        self.queue.put(("Aggregates", overall_dicts))

    def _aggregate_locs(self, logger_pdu: LoggerPDU, base_data: dict):
        aggregate_locs = base_data | {
            "AggregateID": str(
                f"{logger_pdu.pdu.aggregateID.simulationAddress.site}:{logger_pdu.pdu.aggregateID.simulationAddress.application}:{logger_pdu.pdu.aggregateID.aggregateID}"),
            "ForceId": int(logger_pdu.pdu.forceID),
            "AggregateFormation": int(logger_pdu.pdu.formation),
            "dimensionsX": float(logger_pdu.pdu.dimensions.x),
            "dimensionsY": float(logger_pdu.pdu.dimensions.y),
            "dimensionsZ": float(logger_pdu.pdu.dimensions.z),
            "GeolocationX": float(logger_pdu.pdu.centerOfMass.x),
            "GeolocationY": float(logger_pdu.pdu.centerOfMass.y),
            "GeolocationZ": float(logger_pdu.pdu.centerOfMass.z),
            "VelocityX": float(logger_pdu.pdu.velocity.x),
            "VelocityY": float(logger_pdu.pdu.velocity.y),
            "VelocityZ": float(logger_pdu.pdu.velocity.z)
        }
        for k, v in aggregate_locs.items():
            if isinstance(v, float) and math.isnan(v):
                aggregate_locs[k] = float(0.0)

        aggregate_locs = self.replace_nans(aggregate_locs)
        a_id = aggregate_locs["AggregateID"]
        seconds_partition = math.floor(self.n * aggregate_locs["PacketTime"])
        if a_id in self.agr_locs_cache:
            if seconds_partition > self.agr_locs_cache[a_id]:
                self.agr_locs_cache[a_id] = seconds_partition
                self.queue.put(("AggregateLocations", [aggregate_locs]))
        else:
            self.agr_locs_cache[a_id] = seconds_partition
            self.queue.put(("AggregateLocations", [aggregate_locs]))


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

    logger_file = config_data["logger_file"]
    db_name = config_data["database_name"]
    new_db = config_data["new_database"]

    start_time = time.perf_counter()
    end_time = time.perf_counter()
    print(f"Execution time: {datetime.timedelta(seconds=(end_time - start_time))}")
    print("""
    ============================================================
    PLEASE WAIT FOR THE WINDOW TO CLOSE ITSELF
    ============================================================
    """)
