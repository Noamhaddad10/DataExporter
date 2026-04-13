#DIS Real-Time DataExporter Server

This project is a high-performance **DIS(Distributed Interactive Simulation) data export**  
designed to capture simulation network traffic and store it in a **SQL Server database**  in real time.

The System is optimized to process **very high message throughput** using **Python multiprocessing** and an 
asynchronous processing pipeline.

---
#Key Features
**High Throughput Architecture**
Handles large volumes of DIS messages in real time.

**MultiPorcessing PipeLine**
Multiple process parse and export data concurrently.

**SQL Server Integration**
Structured storage using SQLAlchemy ORM.

**UDP DIS Message Capture**
Receives simulation network traffic directly.

**Modular Architecture**
Easy to maintain and extend

#System Architecture

The server process incoming DIS messages through a multi-stage pipeline.

```mermaid
flowchart TD

A[DIS Simualtion NetWork] --> B[UDP Receiver]

B --> C[Message Queue]

C --> D[Parsing Workers<br>(MultiProcessing)]

D --> E[ProcessedQueue] 

E --> F[Export Workers<br>(DataBase Write)]

F --> G[(SQL Server DataBase)] 
```
This Architecture ensures that **network reception , 
parsing, and database export remain decoupled**,
preventing bottlenecks and reducing message loss.

#Supported DIS Messages

The exporter currently supports the following DIS PDU types:

**-EntityState** <br>
**-FirePdu** <br>
**-DetonationPdu**<br>
**-AggregateStatePdu**<br>
**-EventReportPdu**<br>

(You can add the type that you want if it figures into the `pduFactory.py`) <br>
Note: you will have to adjust the code to the new PduType


All runtime parameters are defined in the file:

`DataExporterConfig.json`

#Performance Strategy 

The exporter is designed to support **very High message throughput**

Key design principles:

    - MultiProcessing architecture
    - Separate parsing and export stages
    - Queue buffering between pipeline stages
    - Non-blocking UDP reception

This design ensures that **DataBase slowdowns do not affect network reception.**


#Running the Code:
Start the server with the run of:<br>
    `logger.py`<br>
1.Configuration: `DataExporterConfig.json` <br>
2 Initialize database configuration  <br>
3.Start UDP receiver
4.Spawn parsing workers
5.Spawn export workers
6.Begin processing incoming DIS messages

#Extending the System 
To support new DIS PDU types:
1. add into `logger.py` into the pdu_type_list the num of the pdu type(you can see the number into `PduFactory.py`)
2. into `LoggerPduProcessor.py` into the function process you need to add a process function to the new type

#Maintenance Note

Keys areas to understand when maintaining this project:

1.MultiProcessing worker architecture <br>
2.Queue communication between pipeline stages <br>
3.DataBase performance under heavy load <br>
4.DIS message parsing logic

Any modification to the pipeline should preserve the decoupled architecture to avoid performance degradation

#Author
##Noam Haddad
**Data Engineer - Real-Time Systems Specialized in high-performance data pipelines and simulation data processing**