DataExporter
=============

Welcome to the DataExporter, and Logger of the Battle Lab.  

This code was created by Gidon Rosalki, with version 1.0 being released on 2022.08.23.
and was updated by Matan Ben Shimol.
The purpose is to provide logging of a simulation, following the DIS standard, and to replace the logger from MAK, and the LoggerSQLExporter created to export said files.  
----------------------------

## Starting a new experiment
I shall take you through this procedure, to grant you the knowledge of with what you are dealing, and how to do so.

1. DataExporterConfig.json
   - One must set all the fields within this file. 
   - "exercise_id" - this should be 21 for production, because the all the computers in the experiments' hall are on 21, if you are developing, and you are running some old logger (mak or lzma) or sending your own messages youe should use a different exercise id (make sure no one else use it) 
   - "logger_file" - the name of the loggerfile the program will output and all the pdu's who'll be received will be attached with this loggerfile (every table has a loggerfile column and every database should have loggers table)
   - "data_team_dir" - the path to the network directory which at the end of the program the loggerfile will be moved to (inside a dir with the database name)
   - "notepad_path" - a path to the notepad ++ exe , at the end of the program the error log opens with notepad ++.
   - "entity_locations_per_second" - how many locations per second to insert to the database 
   - "export_to_sql" - if you want to export the data to the sql server (can't think of any reason for it to be false)
   - "tracked_tables" - should be like: "table1,table2,...,tableN".
   - "database_name" - the name of the target database
   - "new_database" - if the database needs to be created or not , if set to true the program will run the createSQL code (it's a copy from the CreateSqlDB project)
   - "message_length" - field can usually be ignored
2. If you haven't made changes to the event reports since you opened your database put the encoder directory output from CreateSQLDB in the `encoders` directory on the target machine, else Run update_pduencoder.py on a machine that has UniSim. This:
   1. Creates the PduEncoder.json file
   2. Creates the SQL code to make the SQL tables
   3. Makes the SQL tables (asks you if you want to alter old tables and it creates the new ones)
3. Put the generated sub-directory of `encoders` into the `encoders` directory on the target machine
4. You may now run logger.exe or the DataExporter

----------------------------
## Using logger.exe
When using the logger, one simply runs it and waits.  
if the loggerfile name you put in the config already exits in the database the program will propose a different name, and you'll need to confirm it in order to continue.
To stop it, one clicks within the window, presses CTRL+c, and waits. The window will close itself and the error log will open itself.

----------------------------

## Building logger.exe
In order to build a distributable exe, one simply runs build_exe.py. Hopefully this will not need to be done regularly.  
build_exe.py makes use of PyInstaller, which can be a pain in the backside. If it no longer builds, and you don't know why, I wish you luck. It took me most of a day before it built for the first time.

----------------------------
# Overall explanation

## logger.py
The logger.py file is home to the class `DataWriter` and the functions `receiving_messages`, `parsing_messeges`, `writing_messages`.

The `DataWriter` should be used with the python `with` statement.
`DataWriter` was designed this way to ensure that no matter what, the file and ports will be properly closed when you are done with them (and the logger file will be moved to the network directory).  

The `receiving_messages` function is a target function for a separate process.
the function receives messages through a udp socket and put them with the time they arrive at in a multiprocessing queue.
this function should run in a separate process because we don't want to miss any message while the program is busy with processing or exporting other messages.

The `parsing_messages` function also runs in a separate process (even more than once), it checks if the pdu is in the wanted pdu type list (usually entity state, fire , detonation and event reports)
and it tries to create a `LoggerPdu` object that contains the packet time and the Pdu object (from opendis.dis7).
when it is done with creating the `LoggerPdu` object  it checks if the received pdu has the wanted exercise id and if so it puts it in another queue for further processing and exporting and a second queue for writing in the lzma file.

The `writing_messages` function runs as a thread and uses the `DataWriter` to write the messages to the lzma file.
The function runs in a loop until a threading event is set, and retrieves data from the writing queue.

In the main process the program creates the writing thread, `LoggerSQLExporter` and `LoggerPduProcessor` objects and  all the different processes.
After that the program enters a loop that retrieve `LoggerPDU`s and process them with the `LoggerPduProcessor`.
the whole loop is wrapped around a try and except block in order to export all the data that the exporters has and  close all the processes gracefully.

## LoggerPduProcessor.py
This file is home to 4 classes: `Exporter`, `LoggerPDU`, `EventReportInterpreter`, and `LoggerSQLExporter`.
`LoggerPDU`: This is a wrapper on all the PDU classes from the opendis library. It adds the necessary field of `packet_time`.  
`EventReportInterpreter`: This interprets any given event report from the received bytes into useful data, based off the data in the PDUEncoder. This standardised format can then be exported as necessary.  
`LoggerPduProcessor`: This class manages all the conversion of data into formats that SQL understands, and passes them on to the  instances of `LoggerSQLExporter`, to be sent to SQL.  

## LoggerSQLExporter.py
This file is home to 4 classes: `Exporter`, and `LoggerSQLExporter`.  
`Exporter`: This class has numerous generated instances throughout the code. An instance is generated for every table to which data is being exported, and it dumps the data it receives to that table on a separate thread from the main thread.
`LoggerSQLExporter`: This class passes the data on to the relevant instances of `Exporter`, to be sent to SQL.  


## update_pduencoder.py
This code is made for updating the Database and create an updated encoder based on the changes made in the EventReports.CS
Before Running this code you should change the database in the `DataExporterConfig.json` to the database you want to update and make encoder for
and the regions, required event reports  in create_PDUEncoder.config.py that you want to be in the database.
First, the program saves all the triggers in `triggers.sql` because some tables will be recreated and the triggers will be dropped as well (the triggers may not work well with the new tables, because some columns might be dropped or added)
The program will ask you if you want to drop all the dis tables before running the sql code from the encoder (this sql code creates tables that don't exist in the database and recreate those who don't have data)
Based on the configs the program will create a new encoder in the `encoders` dir and add the new tables, then it compares the new encoder with the database and checks if changes were made to old tables.
If you replay no to the first question ,and you still want the program to automatically change the old tables (without losing there data) answer no to the second question,
the program will change the existing tables (remove columns and add new ones according to the changes made in the EventReports.CS).
At the end the program will try to recreate all the triggers (some errors might occur in this step).
----------------------------

## Necessary directories
`create_PDUEncoder` houses the code that creates encoders based off what is found in UniSim. It also houses the `GeneralStructs.csv` file, for building such things.     
`encoders` houses the encoders that have been generated.  
`logs` These are the generated logs by the logger.

These are all necessary for running the code, even if the code is a standalone exe. `create_PDUEncoder` however, does not need all the code, just the `GeneralStructs.csv` file.  
This is because the target computer does not have BlSim installed, and should *_NOT_* be trying to generate the PDUEncoder, or the SQL database 

## general things to consider
- This program uses a lot of process (currently 5) and it shouldn't run on a machine that has fewer cpus than said process amount.
- Another thing to consider is the amount of multiprocessing queue this program uses (currently 3), I did some experimenting and found out that to many queues can really impact the program performance (multiprocessing queues are shared memory objects and managing them is very expensive)
maybe the writing thread can run in the receiving messages process or parsing process (won't work if there are more than one, and currently there are 3) and then the writing queue will become redundant.

- Some time in the near future when an experiment with huge amount of entities (and therefore messages) will arrive this version of the data exporter won't stand in the load of all the message 
(this version still can't run on a scenario like those who came in fp without a limit to the amount of location per second), if the recommendations above won't help i recommend you to create a new data exporter
bases on a message brokers like kafka or RabbitMQ, thees can run on a cluster of machines and use more than one machine to process and export the data (in this case you will still use one machine to receive the message, or you will define some messages to be received by some machines , 
like a machine that receives all the entity state pdu and second machine for DetonationPdu etc. or the development team will need to send some messages specifically to some machines based on the entity or the pdu type like I said before.)
