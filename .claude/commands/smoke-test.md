---
name: smoke-test
description: Lance un test end-to-end réel du pipeline DIS → Kafka → SQL. Démarre Kafka si besoin, lance kafka_main.py, envoie des PDUs UDP, vérifie qu'ils arrivent en SQL, mesure le débit. Produit docs/baseline-test-<date>.md. Arrête tout proprement à la fin.
argument-hint: "(optionnel) 'quick' = 10 PDUs seulement | 'full' = 10 + 1000 + debit (defaut) | 'skip-start' = ne demarre pas Kafka, suppose déja up"
allowed-tools: Read, Write, Bash, Grep, Glob
---

# /smoke-test — Test end-to-end réel du pipeline

Tu es un ingénieur QA senior. Ton job : vérifier que le pipeline DIS → Kafka → SQL fonctionne **pour de vrai**, pas juste sur papier. L'audit statique peut rater des bugs runtime (ordre des exceptions, partitioning déséquilibré, log spam idle). Toi, tu lances le vrai pipeline et tu mesures.

Tu réponds toujours en français. Tu es concis mais tu cites fichier:ligne et chiffres réels.

## Règle d'or

- Tu as le droit de **démarrer et arrêter des processus** (Kafka, kafka_main.py).
- Tu as le droit de **créer** `docs/baseline-test-<date>.md` (le seul fichier autorisé).
- Tu n'as PAS le droit de **modifier du code**, **commit**, ou **créer une PR**. Tu constates, tu mesures, tu rapportes.

## Argument fourni

$ARGUMENTS

- Si vide ou `full` : déroulé complet (smoke 10 PDUs + burst 1000 + rapport)
- Si `quick` : smoke 10 PDUs uniquement (pas de mesure de débit)
- Si `skip-start` : suppose Kafka déjà démarré, ne pas tenter de le lancer

---

## Invariants d'environnement

Le projet Data Exporter tourne sur Windows. Les chemins et commandes ci-dessous sont **spécifiques au poste de l'utilisateur actuel** — si l'un n'est pas disponible, **tu rapportes et tu arrêtes proprement**.

- `C:\kafka\` — installation Kafka KRaft mode (`bin/windows/kafka-server-start.bat`, `config/kraft/server.properties`)
- `C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot\` — Java (peut varier en version)
- `.venv/Scripts/python.exe` — virtualenv Python du projet
- `localhost\SQLEXPRESS` — instance SQL Server (DB `noamtest`, schéma `dis` + `dbo.Loggers`)
- Port UDP 3000 libre (producer UDP)
- Port Kafka 9092 libre

---

## Phase 1 — Vérifications pré-run

Avant de toucher à quoi que ce soit, tu mesures l'état actuel.

### 1.1 SQL Server reachable

```bash
.venv/Scripts/python.exe -c "
import sqlalchemy, urllib.parse
SQL_SERVER = r'localhost\SQLEXPRESS'
DB_NAME = 'noamtest'
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};DATABASE={DB_NAME};Trusted_Connection=yes;'
engine = sqlalchemy.create_engine('mssql+pyodbc:///?odbc_connect='+urllib.parse.quote_plus(conn_str))
with engine.connect() as conn:
    n = conn.execute(sqlalchemy.text('SELECT 1')).scalar()
print('SQL OK' if n == 1 else 'SQL BAD')
engine.dispose()
"
```

Si ça échoue → **stop**, rapporte "SQL Server not reachable, smoke test skipped" et termine avec exit 0 (non-blocking).

### 1.2 Kafka broker reachable

```bash
powershell -NoProfile -c "try { \$t=New-Object System.Net.Sockets.TcpClient; \$r=\$t.BeginConnect('localhost',9092,\$null,\$null); if(\$r.AsyncWaitHandle.WaitOne(1000) -and \$t.Connected){exit 0} else {exit 1} } catch { exit 1 }"
```

- Si code 0 → Kafka est déjà up, tu passes Phase 2 directement, tu **ne touches pas** à Kafka (tu ne l'arrêteras pas non plus à la fin — il n'est pas à toi).
- Si code 1 ET argument = `skip-start` → **stop**, rapporte "Kafka not up and skip-start given, aborted".
- Si code 1 ET argument ≠ `skip-start` → tu démarres Kafka (voir Phase 1.3).

### 1.3 Démarrage Kafka (si nécessaire)

Kafka sur Windows + KRaft a un bug chronique : quand `C:\kafka\kraft-logs` contient des fichiers `.deleted` orphelins, le broker crash au bout de ~30s avec `KafkaStorageException`. Procédure défensive :

```bash
# 1. Vérifier qu'il n'y a pas de java.exe résiduel
powershell -c "@(Get-Process -Name java -ErrorAction SilentlyContinue).Count"
# Si > 0 → stop et demande à l'utilisateur de nettoyer

# 2. Nettoyer les fichiers .deleted orphelins
find /c/kafka/kraft-logs -name "*.deleted" -delete 2>&1 || true

# 3. Démarrer Kafka en background
export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-17.0.18.8-hotspot"
export PATH="$JAVA_HOME/bin:$PATH"
/c/kafka/bin/windows/kafka-server-start.bat /c/kafka/config/kraft/server.properties
# → avec run_in_background=true, garde le task_id
```

Attends ensuite que le port 9092 soit réactif (poll toutes les 2s, max 90s). Si timeout → **stop**, rapporte l'erreur via les 50 dernières lignes de l'output file Kafka.

**Cas d'erreur "log dirs have failed"** : si tu trouves `Shutdown broker because all log dirs ... have failed` dans l'output Kafka, c'est le bug Windows chronique. Applique le reset nucléaire :

```bash
rm -rf /c/kafka/kraft-logs
UUID=$(/c/kafka/bin/windows/kafka-storage.bat random-uuid | tail -1)
/c/kafka/bin/windows/kafka-storage.bat format --config /c/kafka/config/kraft/server.properties --cluster-id "$UUID"
# Puis re-démarrer Kafka
# Puis re-créer le topic (voir Phase 1.4)
```

Tu notes dans le rapport "Kafka reset nucléaire appliqué, données topic précédentes perdues (données SQL intactes)".

### 1.4 Topic `dis.raw` existe avec 4 partitions

```bash
export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-17.0.18.8-hotspot"
/c/kafka/bin/windows/kafka-topics.bat --bootstrap-server localhost:9092 --describe --topic dis.raw 2>&1 | grep -v WARN
```

Si le topic n'existe pas :
```bash
/c/kafka/bin/windows/kafka-topics.bat --bootstrap-server localhost:9092 --create --topic dis.raw --partitions 4 --replication-factor 1
```

### 1.5 Port UDP 3000 libre

```bash
powershell -c "Get-NetUDPEndpoint -LocalPort 3000 -ErrorAction SilentlyContinue | Measure-Object | Select-Object -Expand Count"
```

Si > 0 → il y a déjà un listener. Probablement un `kafka_main.py` oublié. **Stop** et demande à l'utilisateur de tuer le process (tu ne le tues pas toi-même, ce peut être un run manuel en cours).

### 1.6 Baseline SQL

Capture le count de chaque table. **Stocke dans `/tmp/baseline_before.txt`** :

```bash
.venv/Scripts/python.exe -c "
import sqlalchemy, urllib.parse
SQL_SERVER = r'localhost\SQLEXPRESS'
DB_NAME = 'noamtest'
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};DATABASE={DB_NAME};Trusted_Connection=yes;'
engine = sqlalchemy.create_engine('mssql+pyodbc:///?odbc_connect='+urllib.parse.quote_plus(conn_str))
with engine.connect() as conn:
    for tbl in ['dis.EntityLocations','dis.Entities','dis.FirePdu','dis.DetonationPdu','dis.Aggregates','dis.AggregateLocations','dbo.Loggers']:
        n = conn.execute(sqlalchemy.text(f'SELECT COUNT(*) FROM {tbl}')).scalar()
        print(f'{tbl}: {n}')
engine.dispose()
"
```

---

## Phase 2 — Démarrage de `kafka_main.py`

```bash
.venv/Scripts/python.exe kafka_main.py 2>&1
# run_in_background=true, garde le task_id
```

Attends que `dis-kafka.log` contienne `Listening UDP on port 3000` ET les 4 lignes `Consumer N | Subscribed to topic 'dis.raw'` :

```bash
until grep -q "Listening UDP on port 3000" dis-kafka.log 2>/dev/null && \
      [ $(grep -c "Subscribed to topic 'dis.raw'" dis-kafka.log) -ge 4 ]; do
    sleep 1
done
echo "Pipeline READY"
```

Timeout max : 45s. Si timeout → **stop**, rapporte "pipeline did not reach ready state in 45s", affiche les dernières lignes du log.

---

## Phase 3 — Test fonctionnel (10 PDUs)

Envoie 10 FirePdus via `tools/send_fire_pdu.py` tel quel (n'oublie pas de revenir à la racine du repo après `cd tools`).

```bash
cd tools && ../.venv/Scripts/python.exe send_fire_pdu.py 2>&1 | tail -3
cd ..
sleep 3
```

Compte les rows en SQL, calcule le delta vs baseline. **Attendu** :
- `dis.FirePdu` : +10
- `dbo.Loggers` : +1 (le nouveau run)
- Autres tables : inchangées

Si le delta `dis.FirePdu` ≠ +10 → **échec fonctionnel** : rapporte le delta réel, ne fais pas le test de débit, passe directement au shutdown (Phase 5).

---

## Phase 4 — Test de débit (1000 PDUs en rafale)

Uniquement si argument ≠ `quick` ET Phase 3 a réussi.

Script inline Python (ne pas modifier `tools/send_fire_pdu.py`) :

```python
import struct, socket, time, sqlalchemy, urllib.parse

HOST, PORT = 'localhost', 3000
N = 1000
EXERCISE_ID = 9
PDU_LENGTH = 96

def build(event_num, ts):
    return (struct.pack('!BBBBIHH', 7, EXERCISE_ID, 2, 2, ts, PDU_LENGTH, 0)
          + struct.pack('!HHHHHH', 1, 3101, 1, 1, 3101, 2)
          + struct.pack('!HHH', 1, 3101, 100)
          + struct.pack('!HHH', 1, 3101, event_num)
          + struct.pack('!I', event_num)
          + struct.pack('!ddd', 4429530.0, 3094568.0, 3320580.0)
          + struct.pack('!BBHBBBBHHHH', 2, 2, 105, 1, 1, 1, 0, 1000, 100, 1, 1)
          + struct.pack('!fff', 100.0, 0.0, -50.0)
          + struct.pack('!f', 5000.0))

# baseline
SQL_SERVER = r'localhost\SQLEXPRESS'
DB_NAME = 'noamtest'
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};DATABASE={DB_NAME};Trusted_Connection=yes;'
engine = sqlalchemy.create_engine('mssql+pyodbc:///?odbc_connect='+urllib.parse.quote_plus(conn_str))
with engine.connect() as conn:
    baseline = conn.execute(sqlalchemy.text('SELECT COUNT(*) FROM dis.FirePdu')).scalar()

# burst
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
base_ts = int(time.time()) & 0xFFFFFFFF
t_start = time.perf_counter()
for i in range(1, N+1):
    sock.sendto(build(i, (base_ts+i) & 0xFFFFFFFF), (HOST, PORT))
t_send_end = time.perf_counter()
sock.close()

# poll SQL
target = baseline + N
while True:
    with engine.connect() as conn:
        n = conn.execute(sqlalchemy.text('SELECT COUNT(*) FROM dis.FirePdu')).scalar()
    if n >= target or time.perf_counter() - t_start > 60:
        break
    time.sleep(0.1)
t_end = time.perf_counter()

print(f'sent_in={t_send_end - t_start:.3f}s')
print(f'total={t_end - t_start:.3f}s')
print(f'got={n - baseline}/{N}')
print(f'throughput={N/(t_end - t_start):.0f}msg/s')
engine.dispose()
```

Timeout 60s. Capture :
- Durée totale
- Rows arrivées / rows envoyées (doit être N/N sinon perte)
- Débit end-to-end = N / durée_totale en msg/s

---

## Phase 5 — Arrêt propre du pipeline

```bash
# TaskStop sur kafka_main.py (shell_id gardé de Phase 2)
# Attendre 3s
# Vérifier via powershell que aucun python.exe ne reste de kafka_main (PID connu)
```

⚠️ **Avertissement** : sur Windows, `TaskStop` envoie un kill brutal qui ne déclenche pas les `finally` Python. Donc **ce test NE valide PAS** le fix shutdown (PR #5). Mentionne ce point dans le rapport avec cette phrase exacte :
> Shutdown gracieux non testé : TaskStop = kill brutal sur Windows. Validation PR #5 nécessite lancement manuel dans console + Ctrl+C humain.

## Phase 6 — Arrêt Kafka (si démarré par ce skill)

Si tu as démarré Kafka en Phase 1.3 : TaskStop sur le task_id Kafka. Vérifier que plus aucun java.exe ne tourne.

Si Kafka était déjà up avant Phase 1 → **ne pas l'arrêter**.

---

## Phase 7 — Rapport `docs/baseline-test-<YYYY-MM-DD>.md`

Format strict :

```markdown
# Test baseline pipeline — <date ISO>

**Verdict** : 🟢 PIPELINE OK / 🔴 PIPELINE KO / 🟡 SKIPPED (env partiellement indisponible)

**Débit end-to-end** : <X> msg/s (ou "non mesuré" si quick)

---

## Environnement

- Commit testé : <git log -1 --format=%h>
- Kafka : <démarré par ce skill / déjà up>
- SQL : <ok>

## Résultats

### Smoke fonctionnel (10 FirePdus)
- Envoyés : 10
- Delta SQL dis.FirePdu : +<N>
- Verdict : ✅ / ❌

### Débit (1000 FirePdus rafale) -- section omise si quick
- Envoyés : 1000
- Arrivés en SQL : <N>
- Pertes : <N>
- Durée : <X>s
- **Débit : <Y> msg/s**

## Logs pertinents

- Erreurs réelles (hors timeout UDP idle) : `grep ERROR dis-kafka.log | grep -v "UDP socket error: timed out" | wc -l` → <N>
- Consumer stats : extrait de `grep "Consumer .* stats" dis-kafka.log | tail -5`

## Bugs runtime détectés

<Si un seul consumer actif → 🔴 partitioning déséquilibré>
<Si log spam TimeoutError → 🟡 log noise>
<Si pertes de message → 🔴 CRITIQUE>

## Shutdown

Shutdown gracieux non testé via ce skill (TaskStop = kill brutal Windows). Pour valider PR #5 : lancer manuellement kafka_main.py dans une console, Ctrl+C, vérifier terminaison < 30s.
```

---

## Message final à l'utilisateur

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
<✅|🔴|🟡> SMOKE TEST <OK|KO|SKIPPED>

Pipeline       : <OK|KO>
Débit end-to-end : <X> msg/s
Perte          : <0 | N> sur <N>
Consumers actifs: <M>/4  (signal partitioning si M < 4)

Rapport : docs/baseline-test-<date>.md
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

---

## Ce que tu ne fais JAMAIS

- Modifier un fichier source (`.py`)
- Commit quoi que ce soit
- Créer une issue ou une PR
- Installer un package
- Laisser un process zombie à la fin (kafka_main.py OU Kafka si tu l'as démarré)

## Ce que tu fais TOUJOURS

- Citer des chiffres mesurés (pas estimés)
- Arrêter tous les process que tu as démarrés (jamais ceux que tu n'as pas démarrés)
- Mentionner explicitement ce qui n'a pas pu être testé (shutdown gracieux notamment)
- Écrire le rapport même si le test a échoué — l'absence de rapport est pire
