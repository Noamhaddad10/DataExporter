@echo off
setlocal enabledelayedexpansion
chcp 65001 > nul

echo ============================================================
echo  Installation et demarrage Apache Kafka 3.9.0 (KRaft mode)
echo  Sans Zookeeper - Installation native Windows
echo ============================================================
echo.

REM ============================================================
REM [1/5] Verification Java JDK 17
REM ============================================================
echo [1/5] Verification Java...
java -version > nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo ERREUR: Java non trouve dans le PATH.
    echo.
    echo Installez Eclipse Temurin JDK 17 :
    echo   https://adoptium.net/temurin/releases/?version=17
    echo.
    echo Apres installation, ajoutez le bin/ de Java au PATH systeme.
    pause
    exit /b 1
)
java -version
echo Java OK.
echo.

REM ============================================================
REM [2/5] Verification du repertoire Kafka
REM ============================================================
echo [2/5] Verification de C:\kafka...
if not exist "C:\kafka" (
    echo ERREUR: Le repertoire C:\kafka n'existe pas.
    echo.
    echo Telechargez Apache Kafka 3.9.0 :
    echo   https://kafka.apache.org/downloads
    echo Choisissez : kafka_2.13-3.9.0.tgz (Scala 2.13)
    echo Extrayez dans C:\kafka (de sorte que C:\kafka\bin\ existe)
    echo.
    pause
    exit /b 1
)

if not exist "C:\kafka\bin\windows\kafka-server-start.bat" (
    echo ERREUR: kafka-server-start.bat introuvable dans C:\kafka\bin\windows\
    echo Verifiez que l'extraction est correcte.
    pause
    exit /b 1
)
echo Repertoire Kafka OK : C:\kafka
echo.

REM ============================================================
REM [3/5] Ajustement du server.properties pour KRaft mode
REM ============================================================
echo [3/5] Configuration server.properties (KRaft mode)...

REM Fichier de config KRaft
set KRAFT_CONFIG=C:\kafka\config\kraft\server.properties

REM Verification que le fichier existe (Kafka 3.9.0 le fournit)
if not exist "%KRAFT_CONFIG%" (
    echo ERREUR: %KRAFT_CONFIG% introuvable.
    echo Verifiez votre installation Kafka.
    pause
    exit /b 1
)

REM Ajout/mise a jour des parametres recommandes
REM On ecrit un fichier .additions et on l'appende si les cles ne sont pas deja presentes

findstr /C:"log.dirs=C:/kafka/kraft-logs" "%KRAFT_CONFIG%" > nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo log.dirs=C:/kafka/kraft-logs >> "%KRAFT_CONFIG%"
    echo   Ajoute: log.dirs=C:/kafka/kraft-logs
)

findstr /C:"log.retention.hours=24" "%KRAFT_CONFIG%" > nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo log.retention.hours=24 >> "%KRAFT_CONFIG%"
    echo   Ajoute: log.retention.hours=24
)

findstr /C:"num.partitions=4" "%KRAFT_CONFIG%" > nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo num.partitions=4 >> "%KRAFT_CONFIG%"
    echo   Ajoute: num.partitions=4
)

echo Configuration OK.
echo.

REM ============================================================
REM [4/5] Formatage du storage KRaft (une seule fois)
REM ============================================================
echo [4/5] Preparation du storage KRaft...

REM Si le repertoire de logs existe deja, c'est que Kafka a deja ete formate
REM On saute cette etape pour eviter de perdre les donnees
if exist "C:\kafka\kraft-logs" (
    echo Storage deja formate (C:\kafka\kraft-logs existe) — etape ignoree.
    echo Pour reinitialiser : supprimez C:\kafka\kraft-logs et relancez ce script.
) else (
    echo Generation du Cluster ID...
    C:\kafka\bin\windows\kafka-storage.bat random-uuid > C:\kafka\cluster_id.txt 2>&1
    if %ERRORLEVEL% NEQ 0 (
        echo ERREUR lors de la generation du Cluster ID.
        type C:\kafka\cluster_id.txt
        pause
        exit /b 1
    )
    set /p CLUSTER_ID=<C:\kafka\cluster_id.txt
    set CLUSTER_ID=!CLUSTER_ID: =!
    echo Cluster ID : !CLUSTER_ID!

    echo Formatage du storage...
    C:\kafka\bin\windows\kafka-storage.bat format -t !CLUSTER_ID! -c "%KRAFT_CONFIG%"
    if %ERRORLEVEL% NEQ 0 (
        echo ERREUR lors du formatage du storage.
        pause
        exit /b 1
    )
    echo Storage formate avec succes.
)
echo.

REM ============================================================
REM [5/5] Demarrage du broker Kafka
REM ============================================================
echo [5/5] Demarrage du broker Kafka...

REM Verifie si Kafka tourne deja (port 9092)
netstat -an | findstr ":9092 " | findstr "LISTENING" > nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo Kafka semble deja en cours d'execution sur le port 9092.
    echo Si ce n'est pas le cas, verifiez les processus Java actifs.
    goto :create_topic
)

REM Demarrage dans une nouvelle fenetre
start "Kafka Broker" cmd /k "C:\kafka\bin\windows\kafka-server-start.bat C:\kafka\config\kraft\server.properties"

echo Attente du demarrage du broker (15 secondes)...
timeout /t 15 /nobreak > nul

REM Verification que le broker repond
netstat -an | findstr ":9092 " | findstr "LISTENING" > nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo AVERTISSEMENT: Le port 9092 ne semble pas encore ecouter.
    echo Attendez quelques secondes supplementaires et verifiez la fenetre "Kafka Broker".
)

:create_topic
echo.

REM ============================================================
REM Creation du topic dis.raw
REM ============================================================
echo Creation du topic 'dis.raw' (4 partitions, replication-factor 1)...

REM Verifie si le topic existe deja
C:\kafka\bin\windows\kafka-topics.bat ^
    --describe ^
    --topic dis.raw ^
    --bootstrap-server localhost:9092 > nul 2>&1

if %ERRORLEVEL% EQU 0 (
    echo Topic 'dis.raw' existe deja — ignoré.
) else (
    C:\kafka\bin\windows\kafka-topics.bat ^
        --create ^
        --topic dis.raw ^
        --partitions 4 ^
        --replication-factor 1 ^
        --bootstrap-server localhost:9092

    if %ERRORLEVEL% NEQ 0 (
        echo ERREUR lors de la creation du topic.
        echo Verifiez que le broker est bien demarre.
        pause
        exit /b 1
    )
    echo Topic 'dis.raw' cree avec succes.
)

echo.
echo ============================================================
echo  Kafka est pret !
echo ============================================================
echo  Bootstrap server : localhost:9092
echo  Topic            : dis.raw (4 partitions)
echo  Mode             : KRaft (sans Zookeeper)
echo  Retention        : 24 heures
echo  Logs Kafka       : C:\kafka\kraft-logs
echo.
echo  Pour tester la connexion Python :
echo    .venv\Scripts\python.exe -c "from confluent_kafka import Producer; p = Producer({'bootstrap.servers': 'localhost:9092'}); p.produce('dis.raw', b'test'); p.flush(); print('OK')"
echo.
echo  Pour demarrer le pipeline DIS :
echo    .venv\Scripts\python.exe kafka_main.py
echo ============================================================
echo.

pause
