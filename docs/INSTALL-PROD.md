# Installation du projet Data Exporter sur PC prod (air-gapped)

**Contexte** : le PC de prod n'a pas Internet. Toutes les dépendances sont téléchargées sur un PC relais (avec Internet), transférées sur clé USB / disque, puis installées sur le PC de prod.

**Ce qui est sur le PC prod d'office** : Windows, SQL Server (avec la base `noamtest`, schéma `dis` et ses 6 tables + `dbo.Loggers` déjà créés).

**Ce qu'on doit installer sur le PC prod** : Python, Java, Kafka, ODBC Driver, packages Python, et le code du projet.

**Durée estimée** :
- Phase 1 (téléchargement PC relais) : ~30 min (selon ta connexion)
- Phase 2 (transfert) : quelques minutes
- Phase 3 (installation prod) : ~30 min
- Phase 4 (vérification) : ~10 min

---

## Architecture de ce qu'on va transférer

```
transfer-kit/                          ← dossier à copier sur clé USB
├── installers/
│   ├── python-3.10.11-amd64.exe
│   ├── OpenJDK17U-jdk_x64_windows.msi
│   ├── msodbcsql17.msi
│   └── kafka_2.13-3.9.0.tgz
├── python-offline-packages/           ← wheels pré-téléchargés
│   ├── confluent_kafka-*.whl
│   ├── sqlalchemy-*.whl
│   ├── pyodbc-*.whl
│   ├── pandas-*.whl
│   ├── opendis-*.whl
│   └── [toutes les dépendances transitives]
├── data-exporter/                     ← le projet (code source)
│   ├── kafka_main.py
│   ├── kafka_producer.py
│   ├── kafka_consumer.py
│   ├── kafka_config.py
│   ├── LoggerPduProcessor.py
│   ├── LoggerSQLExporter.py
│   ├── requirements.txt
│   ├── tools/
│   ├── tests/
│   └── docs/
└── DataExporterConfig.json.template   ← modèle à éditer
```

---

## Phase 1 — Téléchargement sur PC relais (avec Internet)

Toutes les étapes ci-dessous se font sur **un PC avec Internet** (ton PC personnel par exemple).

Crée un dossier racine :

```bash
mkdir C:\transfer-kit
cd C:\transfer-kit
mkdir installers
mkdir python-offline-packages
```

### 1.1 Python 3.10 installer

Version à utiliser : **3.10.11** (la même que ton env de dev).

Télécharge l'installer Windows 64 bits depuis :
→ https://www.python.org/downloads/release/python-31011/
→ Fichier : `Windows installer (64-bit)` — `python-3.10.11-amd64.exe`

Place le dans `installers/`.

> **Pourquoi 3.10 précisément** : le projet utilise `bytes | None` dans les annotations de type (Python 3.10+). Rester sur la même version que le dev évite les surprises.

### 1.2 Java JDK 17 (Temurin/Eclipse Adoptium)

Nécessaire pour Kafka (en mode KRaft, pas Zookeeper).

Télécharge le MSI Windows x64 depuis :
→ https://adoptium.net/temurin/releases/?version=17
→ Sélectionne : Windows x64, JDK 17 LTS, `.msi`
→ Fichier : `OpenJDK17U-jdk_x64_windows_hotspot_17.0.<xx>.msi`

Place le dans `installers/`.

### 1.3 Apache Kafka 3.9.0

Télécharge depuis :
→ https://kafka.apache.org/downloads
→ Sous "Scala 2.13" : `kafka_2.13-3.9.0.tgz`

Place le dans `installers/`.

### 1.4 Microsoft ODBC Driver 17 for SQL Server

Nécessaire pour que `pyodbc` puisse parler à SQL Server.

Télécharge depuis :
→ https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
→ Version : 17.x, Windows x64, `.msi`
→ Fichier : `msodbcsql.msi` (ou similaire selon la version exacte)

Place le dans `installers/`.

### 1.5 Packages Python offline

Installe d'abord Python 3.10.11 localement sur ce PC relais **si ce n'est pas déjà fait**, puis depuis un terminal :

```powershell
# Télécharge les wheels pour Python 3.10 Windows 64-bit
python -m pip download ^
    -r <chemin-vers-ton-clone-du-projet>\requirements.txt ^
    -d C:\transfer-kit\python-offline-packages ^
    --platform win_amd64 ^
    --only-binary=:all: ^
    --python-version 3.10
```

⚠️ **Important** :
- `--platform win_amd64` force les wheels Windows
- `--only-binary=:all:` refuse les packages qui nécessitent compilation (donc pas besoin de compilateur C sur prod)
- `--python-version 3.10` cible ta version prod

Si cette commande échoue sur un package (pas de wheel binaire disponible), télécharge-le manuellement depuis https://pypi.org/ en choisissant la version `cp310` et `win_amd64`.

Vérifie le résultat :

```powershell
Get-ChildItem C:\transfer-kit\python-offline-packages\*.whl | Measure-Object
# Tu devrais avoir ~15-25 fichiers .whl (dépendances transitives incluses)
```

### 1.6 Le projet Data Exporter

Depuis ton PC relais (qui a déjà le projet) :

```bash
cd C:\Users\hadda\Desktop\WiresharkLogger
# Copie tout SAUF les fichiers non nécessaires
robocopy . C:\transfer-kit\data-exporter /E /XD .git .venv __pycache__ build .idea logs output .claude /XF "*.log" "*.exe" "*.spec"
```

Ou en git :

```bash
git clone --depth 1 https://github.com/Noamhaddad10/DataExporter.git C:\transfer-kit\data-exporter
# puis supprimer .git si tu ne veux pas transporter l'historique
```

### 1.7 Modèle de `DataExporterConfig.json`

Le fichier `DataExporterConfig.json` est gitignored (peut contenir des valeurs spécifiques à une machine). Prépare un template :

```powershell
copy C:\Users\hadda\Desktop\WiresharkLogger\DataExporterConfig.json C:\transfer-kit\DataExporterConfig.json.template
```

Ouvre le template et remets les valeurs par défaut pour prod (voir phase 3.5 plus bas).

---

## Phase 2 — Transfert

Copie le dossier `C:\transfer-kit` sur une clé USB / disque externe / partage réseau autorisé, puis transfère-le sur le PC de prod.

Destination sur prod (exemple) : `C:\deploy\transfer-kit\`

---

## Phase 3 — Installation sur PC prod

Toutes les étapes ci-dessous se font **sur le PC de prod (sans Internet)**, à partir du `transfer-kit` transféré.

### 3.1 Installer Python 3.10.11

```
C:\deploy\transfer-kit\installers\python-3.10.11-amd64.exe
```

Dans l'assistant :
- ✅ **Cocher "Add Python 3.10 to PATH"**
- Install Now (ou Customize pour changer le chemin)

Vérifie en ouvrant un nouveau PowerShell :

```powershell
python --version
# → Python 3.10.11
```

### 3.2 Installer Java 17

Exécute `OpenJDK17U-jdk_x64_windows_hotspot_17.0.<xx>.msi`. Accepte les défauts. Il s'installe par défaut dans `C:\Program Files\Eclipse Adoptium\jdk-17.0.<xx>-hotspot\`.

Vérifie :

```powershell
java -version
# → openjdk version "17.0.xx"
```

### 3.3 Installer ODBC Driver 17

Exécute `msodbcsql.msi`. Accepte la licence. Install par défaut. Aucune config particulière.

### 3.4 Extraire Kafka

```powershell
# Extraction de kafka_2.13-3.9.0.tgz (utilise 7-Zip ou WinRAR, en deux étapes si besoin : tgz → tar → dossier)
# Résultat final attendu :
C:\kafka\
├── bin\windows\
├── config\
├── libs\
└── ...
```

Vérifie que `C:\kafka\bin\windows\kafka-server-start.bat` existe.

### 3.5 Déployer le projet

```powershell
# Copie le projet dans un dossier stable
xcopy /E /I C:\deploy\transfer-kit\data-exporter C:\WiresharkLogger

# Place le fichier de config (adapte les valeurs)
copy C:\deploy\transfer-kit\DataExporterConfig.json.template C:\WiresharkLogger\DataExporterConfig.json
notepad C:\WiresharkLogger\DataExporterConfig.json
```

Édite `DataExporterConfig.json` :

```json
{
    "exercise_id": <ID_exercice_prod>,
    "logger_file": "exp_<scenario>.lzma",
    "data_team_dir": "output",
    "notepad_path": "<chemin notepad si tu l'utilises>",
    "entity_locations_per_second": 1,
    "export_to_sql": true,
    "database_name": "<nom_base_prod>",
    "tracked_tables": "",
    "new_database": false,
    "message_length": 262144,
    "Scenario": <num_scenario>,
    "site_id": 1,
    "app_number": 3101,
    "export_delay": 0.01,
    "export_size": 1500,
    "BatchSize": 256,
    "BatchTime": 0.03,
    "HEADER_SIZE": 4096,
    "SLOT_SIZE": 8192,
    "N_SLOT": 100000,
    "SLOT_HDR": 20,
    "PORT": 3000,
    "sql_server": "<nom_du_serveur_SQL_prod>\\<instance_si_applicable>",
    "kafka": {
        "enabled": true,
        "bootstrap_servers": "localhost:9092",
        "topic": "dis.raw",
        "num_partitions": 4,
        "group_id": "dis-export-group",
        "num_consumers": 4,
        "producer": {
            "linger_ms": 5,
            "batch_size": 65536,
            "compression_type": "lz4",
            "acks": "all"
        },
        "consumer": {
            "auto_offset_reset": "earliest",
            "fetch_min_bytes": 1024,
            "fetch_wait_max_ms": 100,
            "max_poll_interval_ms": 300000,
            "session_timeout_ms": 30000
        }
    }
}
```

**Points d'attention sur la config prod** :
- `database_name` : le nom de ta base SQL prod
- `sql_server` : l'adresse de ton SQL Server prod (ex: `MYPRODSRV\SQLEXPRESS` ou `MYPRODSRV,1433`). Par défaut, le code tombe sur `localhost\SQLEXPRESS` si la clé est absente.
- `exercise_id` et `Scenario` : valeurs métier propres au run
- `kafka.bootstrap_servers` : reste `localhost:9092` si le broker tourne sur la même machine. Sinon mets l'adresse du broker.

### 3.6 Créer le venv et installer les packages Python

```powershell
cd C:\WiresharkLogger
python -m venv .venv

# Active le venv
.venv\Scripts\activate

# Installe tous les packages depuis le dossier offline (aucun accès Internet requis)
pip install --no-index --find-links=C:\deploy\transfer-kit\python-offline-packages -r requirements.txt
```

Vérifie :

```powershell
python -c "from confluent_kafka import Producer; import sqlalchemy, pyodbc, pandas, opendis; print('All imports OK')"
```

Si un package manque → retour en phase 1.5 sur PC relais pour le retélécharger.

### 3.7 Initialiser Kafka (KRaft, un seul broker)

Une seule fois, à la première installation :

```powershell
# Set Java home pour la session
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.<xx>-hotspot"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"

# Génère un cluster UUID
$UUID = & C:\kafka\bin\windows\kafka-storage.bat random-uuid | Select-Object -Last 1

# Formate le storage
C:\kafka\bin\windows\kafka-storage.bat format --config C:\kafka\config\kraft\server.properties --cluster-id $UUID
```

---

## Phase 4 — Vérification de l'installation

### 4.1 Démarrer Kafka

Ouvre un terminal PowerShell **dédié** (à laisser ouvert) :

```powershell
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.<xx>-hotspot"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
C:\kafka\bin\windows\kafka-server-start.bat C:\kafka\config\kraft\server.properties
```

Attends de voir `[KafkaServer id=1] started` (environ 20-40s). **Ne ferme pas cette fenêtre**.

### 4.2 Créer le topic dis.raw

Dans un **autre** PowerShell :

```powershell
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.<xx>-hotspot"
C:\kafka\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic dis.raw --partitions 4 --replication-factor 1
```

Vérifie :

```powershell
C:\kafka\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --describe --topic dis.raw
```

→ 4 partitions, `Leader: 1`, `Replicas: 1`, `Isr: 1`.

### 4.3 Vérifier la connexion SQL

```powershell
cd C:\WiresharkLogger
.venv\Scripts\python.exe -c "import kafka_config as cfg, sqlalchemy, urllib.parse; conn_str=f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={cfg.SQL_SERVER};DATABASE={cfg.DB_NAME};Trusted_Connection=yes;'; e = sqlalchemy.create_engine('mssql+pyodbc:///?odbc_connect='+urllib.parse.quote_plus(conn_str)); [print('SQL OK' if c.execute(sqlalchemy.text('SELECT 1')).scalar() == 1 else 'BAD') for c in [e.connect().__enter__()]]"
```

Attendu : `SQL OK`.

### 4.4 Smoke test (test end-to-end)

Lance le pipeline dans un terminal :

```powershell
cd C:\WiresharkLogger
.venv\Scripts\python.exe kafka_main.py
```

Attends de voir `Press Ctrl+C to stop gracefully`.

Dans un **autre** terminal, envoie 10 FirePdus de test :

```powershell
cd C:\WiresharkLogger
.venv\Scripts\python.exe tools\send_fire_pdu.py
```

Attendu : 10 lignes "FirePdu #N sent".

Vérifie côté SQL :

```sql
SELECT COUNT(*) FROM dis.FirePdu WHERE LoggerFile = 'exp_<scenario>.lzma'
-- devrait montrer +10 rows par rapport à avant
```

Si ✅ → **Installation réussie**.

Retourne au terminal où tourne `kafka_main.py` et fais **Ctrl+C**. Le shutdown doit terminer en < 30s (voir "Graceful shutdown" plus bas). Ferme ensuite la fenêtre Kafka (Ctrl+C dans son terminal).

---

## Phase 5 — Lancement en production

### Séquence de démarrage

1. Ouvre PowerShell → démarre Kafka (phase 4.1)
2. Attends `started`
3. Ouvre un 2ème PowerShell → lance `kafka_main.py`
4. Attends `Press Ctrl+C to stop gracefully`
5. Démarre les émetteurs DIS (le reste de ta chaîne réseau)

### Séquence d'arrêt propre

1. **D'abord arrête les émetteurs DIS** (plus de trafic UDP entrant)
2. Attends 10-30s que les consumers drainent leur backlog. Surveille les lignes `Consumer N stats` dans `dis-kafka.log` : `consumed == exported` et les compteurs stagnent.
3. Dans le terminal de `kafka_main.py` → **Ctrl+C**
4. Attends le message `Pipeline stopped gracefully` (< 30s)
5. Dans le terminal de Kafka → **Ctrl+C**

### Détecter une perte (phase critique)

Vérifie après chaque scénario :

```powershell
# Comparer ce que le producer a envoyé et ce que les consumers ont traité :
Get-Content C:\WiresharkLogger\dis-kafka.log | Select-String "Producer stats | sent="
Get-Content C:\WiresharkLogger\dis-kafka.log | Select-String "Consumer .* stats"
# La somme des `consumed` sur les 4 consumers doit égaler le `sent` du producer.
```

Si tu vois un écart → perte confirmée → consulte `docs/audit-report-2026-04-23-161302.md` et planifie un passage à 3 brokers.

---

## Dépannage

### "Kafka crash avec 'log dirs have failed'"
C'est le bug Windows chronique sur les fichiers `.deleted`. Nettoyage :

```powershell
# Stop Kafka d'abord
Remove-Item "C:\kafka\kraft-logs\*\*.deleted" -Force -ErrorAction SilentlyContinue
# Redémarre Kafka
```

Si ça persiste → reset complet :

```powershell
Remove-Item -Recurse -Force C:\kafka\kraft-logs
$UUID = & C:\kafka\bin\windows\kafka-storage.bat random-uuid | Select-Object -Last 1
C:\kafka\bin\windows\kafka-storage.bat format --config C:\kafka\config\kraft\server.properties --cluster-id $UUID
# Re-créer le topic dis.raw (voir phase 4.2)
```

⚠️ **Attention** : le reset efface tous les messages Kafka (mais PAS les données SQL).

### "pyodbc.InterfaceError: Data source name not found"
ODBC Driver 17 n'est pas installé. Réexécute phase 3.3.

### "Consumer N | Kafka error"
Généralement transient (broker redémarrage, network). Le consumer retente tout seul. Si ça persiste : vérifie que Kafka est bien up avec `kafka-topics.bat --bootstrap-server localhost:9092 --list`.

### "ImportError: DLL load failed pour pyodbc"
Version de pyodbc incompatible avec la version de Python. Télécharger le wheel spécifique à `cp310-win_amd64`.

---

## Annexes

### Structure des logs

- `dis-kafka.log` : tous les événements Python (producer, consumer, main). C'est le fichier à consulter en cas de problème.
- `C:\kafka\logs\` : logs internes de Kafka broker.

### Redémarrage après crash machine prod

Si le PC de prod redémarre :

1. Kafka n'est PAS un service Windows par défaut. Tu dois relancer manuellement `kafka-server-start.bat` au reboot.
2. Idem pour `kafka_main.py`.
3. Les données déjà écrites en SQL sont intactes.
4. Les messages Kafka non encore consommés sont préservés sur disque (`C:\kafka\kraft-logs\`) — ils seront retraités au redémarrage du consumer (garantie at-least-once).

Pour un déploiement plus robuste, envisager NSSM ou un scheduler Windows pour relancer Kafka et `kafka_main.py` automatiquement au boot. Hors scope de ce guide.

### Mise à jour du code (plus tard)

Si tu dois mettre à jour le code depuis ton PC relais :

```powershell
# Sur prod, stop kafka_main.py (Ctrl+C)
# Copie les nouveaux fichiers .py depuis le kit de transfert
xcopy /Y /E C:\deploy\transfer-kit\data-exporter\*.py C:\WiresharkLogger\
# Relance kafka_main.py
```

`DataExporterConfig.json` n'est PAS écrasé (il est dans .gitignore, donc jamais dans le kit de transfert côté code).

---

## Checklist finale avant premier scénario

- [ ] Python 3.10.11 installé, `python --version` OK
- [ ] Java 17 installé, `java -version` OK
- [ ] ODBC Driver 17 installé
- [ ] Kafka extrait dans `C:\kafka\`, formaté, démarre sans erreur
- [ ] Topic `dis.raw` créé avec 4 partitions
- [ ] Venv créé, packages installés, imports OK
- [ ] `DataExporterConfig.json` édité avec les valeurs prod
- [ ] SQL Server accessible (test `SELECT 1` OK)
- [ ] Tables `dis.*` et `dbo.Loggers` présentes dans la base
- [ ] Smoke test (10 FirePdus) → delta +10 en SQL
- [ ] Shutdown propre testé (Ctrl+C → pipeline stopped gracefully < 30s)

Quand tout est ✅ → tu es prêt pour un scénario réel.
