# CLAUDE.md — Project context for Claude sessions

> **Pour toi (Noam)** : ce fichier est chargé automatiquement par Claude au début de chaque session sur ce projet. C'est ma "mémoire externe" pour reprendre exactement là où on en est, même si la conversation est réinitialisée. Je m'engage à le tenir à jour à la fin de chaque session significative.

> **Dernière mise à jour** : 2026-04-27 (Launch DataExporter.bat ajouté, build PyInstaller écarté)

---

## 1. Contexte projet

**Data Exporter** : pipeline temps-réel **DIS 7 → UDP → Kafka → SQL Server** pour capturer du trafic de simulation distribuée et le persister en base.

**Contexte utilisateur (Noam)** :
- Travail dans une organisation avec **réseau interne air-gapped** (sans Internet)
- **Zéro perte de message tolérée** — c'est l'objectif numéro 1, contractuel
- Migration récente de l'ancien système SharedMemory Ring Buffer (`legacy/logger.py`, faisait 20k msg/s en prod) vers **Kafka** pour la durabilité et la robustesse
- Développement actuel sur PC perso sous Windows ; déploiement final sur PC prod air-gapped
- Scénarios : **3 heures max**, puis arrêt complet du pipeline

---

## 2. État de la production

- **1 broker Kafka** unique pour démarrer (le user a tranché). Passage à 3 brokers seulement si perte observée empiriquement.
- **Retention Kafka** : 24h (défaut). Largement suffisant car scénarios de 3h max.
- **Hardware prod** : ~×10-15 plus puissant que le laptop dev. Le débit Kafka actuel (~935 msg/s laptop) devrait monter à ~10-15k/s sur prod, ce qui est suffisant (le user n'avait jamais de problème de débit avec l'ancien système).
- **Fresh-start mode activé** (`reset_topic_on_startup: true`) : chaque lancement de `kafka_main.py` démarre les consumers avec un nouveau `group_id` unique → ignore les anciens scénarios automatiquement, sans toucher au disque (évite le bug Windows `.deleted`).

---

## 3. Architecture / tech stack

| Composant | Version | Notes |
|---|---|---|
| Python | 3.10.11 | Type hints `bytes \| None` utilisés, fixe sur 3.10 |
| Java | 17.0.18 (Temurin) | Pour Kafka KRaft mode |
| Apache Kafka | 3.9.0 (Scala 2.13) | KRaft (sans Zookeeper) |
| SQL Server | 2025 RTM (dev), prod = ? | DB `noamtest` côté dev |
| ODBC Driver | 17 | Windows |
| confluent-kafka | 2.14.0 | Producer/Consumer Kafka |
| sqlalchemy | 2.0.49 | ORM SQL |
| pyodbc | 5.3.0 | Driver ODBC pour sqlalchemy |
| pandas | 2.3.3 | Pour `pd.read_sql_query` dans resolve_logger_file |
| **opendis** | **1.1.0a4+aggregatepdu** | ⚠️ **BUILD CUSTOM**, voir ci-dessous |

### Détail critique — opendis custom build

**Problème** : la version PyPI `opendis 1.0` (la seule release officielle) ne contient **pas la classe `AggregateStatePdu`** (PDU type 33). Le code projet en a besoin (utilisée dans `LoggerPduProcessor.process()`).

**Solution appliquée** : wheel buildée à partir du **commit GitHub `9911bcf9`** du repo `open-dis/open-dis-python` (2025-09-14 — "Marking AggregateStatePDU as complete"). C'est le **dernier commit** où :
- ✅ `AggregateStatePdu` est implémentée
- ✅ L'API legacy `entityID.siteID/applicationID/entityID` est encore en place

Les commits postérieurs (à partir du 2025-10-07) refactorent `EntityID` en `EntityIdentifier` avec sub-objet `simulationAddress`/`entityNumber`, ce qui **casserait** `LoggerPduProcessor.py`.

Version PEP 440 utilisée pour le local build : `1.1.0a4+aggregatepdu` (le `+aggregatepdu` est un local version identifier).

Source archive gardée localement : `C:\DataExporter-prod-test\opendis-source\open-dis-python-9911bcf9...`

### PDU types supportés

- **type 1** : EntityStatePdu (EntityState)
- **type 2** : FirePdu (FirePdu)
- **type 3** : DetonationPdu (DetonationPdu)
- **type 21** : EventReportPdu (mappé par opendis, mais code projet ne le traite pas — silently skipped)
- **type 33** : AggregateStatePdu (AggregateState — critique pour le user)

---

## 4. Décisions clés (chronologique, plus récente en premier)

### 2026-04-27 (suite 2)
- **Logger_file naming sous contrôle du launcher** (commit à venir) :
  - Nouveau flag `kafka_config.DISABLE_LOGGER_FILE_RESOLUTION` (défaut False).
  - `kafka_main.resolve_logger_file()` court-circuitée si flag True : utilise le `logger_file` du config tel quel.
  - Le launcher écrit `disable_logger_file_resolution=true` à chaque Start.
  - Au **Save preset**, le launcher check `dbo.Loggers` : si conflit, popup avec suggestion `_N+1`, user accepte ou refuse (avec warning).
  - Au **Start**, re-check de sécurité : auto-bump silencieux + log clair si conflit nouveau.
  - Banner RUNNING affiche désormais le **vrai** nom (pas le preset, le nom utilisé en SQL).
- **Bug timing fix** (commit dans le launcher) : avant, le launcher disait "Pipeline ready" dès "Listening UDP" (du producer). Mais les 4 consumers prenaient encore ~30s à subscribe. Conséquence : avec `auto.offset.reset=latest`, les PDUs envoyés tôt étaient skippés. Fix : attendre `Subscribed to topic` × N consumers + 5s grace (partition assignment).
- **Kit transfer refresh** (~410 MB) :
  - Ajout PyQt5 + PyQt5-Qt5 + PyQt5-sip dans `python-offline-packages/` (3 wheels supplémentaires)
  - launcher.py + kafka_main.py + kafka_config.py mis à jour dans `data-exporter/`
  - requirements.txt inclut PyQt5
  - MANIFEST.md + INSTALL-PROD.md mis à jour
  - **Test offline strict réussi** : install + import launcher + AggregateStatePdu OK depuis venv 100% offline

### 2026-04-27 (suite)
- **GUI launcher PyQt5** ajouté (`launcher.py` à la racine). Pilote Kafka + kafka_main.py via subprocess.Popen avec `CREATE_NEW_PROCESS_GROUP` (pour pouvoir envoyer `CTRL_BREAK_EVENT` au shutdown).
- Vue **IDLE** (config + start) vs vue **RUNNING** (banner minimal + log live full-screen).
- **Toggle DEV/PROD** : 4 champs persistés par mode dans `LauncherPresets.json` (gitignoré) — `exercise_id`, `port`, `logger_file`, `database_name`. Le launcher écrit ces valeurs dans `DataExporterConfig.json` au Start.
- **Force `reset_topic_on_startup: true`** au démarrage du launcher → garantit fresh-start mode même si la config user ne l'a pas.
- **Theme light/dark** persisté dans `_meta` du presets.json (palette Catppuccin Mocha en dark).
- **DEV tools panel** (visible en mode DEV + RUNNING) : envoi de N FirePdus à un débit configurable via UDP, avec rate limiting précis via `time.perf_counter()`. Panneau caché en PROD pour éviter envoi accidentel.
- **Status polling** (5s) : vérifie `localhost:9092` (Kafka) et `SELECT 1` sur la DB en arrière-plan.
- **Live log tail** : QThread qui suit `dis-kafka.log` ligne par ligne et émet vers le QPlainTextEdit.
- **Build PyInstaller ready** : détection `sys.frozen` → utilise `kafka_main.exe` à côté de `launcher.exe` en mode build, sinon `python.exe + kafka_main.py`.

### 2026-04-27
- **Fresh-start mode** ajouté (`feat: fresh-start mode for scenario isolation between runs`, commit `1e8ddee`). Chaque lancement utilise un `group_id` unique avec timestamp et `auto.offset.reset=latest`. Évite le bug Windows `.deleted` qui se déclenche quand on supprime des segments Kafka.
- Suppression du script `reset-kafka.bat` (devenu inutile).

### 2026-04-26
- **Build kit offline complet** dans `C:\DataExporter-prod-test\transfer-kit\` (~355 MB) :
  - Installeurs Python/Java/Kafka/ODBC pour offline install
  - 13 wheels Python pré-téléchargées (`pip download --only-binary --platform win_amd64 --python-version 3.10`)
  - Code projet copié, requirements.txt corrigé
  - MANIFEST.md + DataExporterConfig.json.template (100 % anglais)
  - `docs/INSTALL-PROD.md` (anglais, complet, 12 phases)
- Cleanup : retiré du kit tout ce qui était dev-interne (`.claude/`, `.github/`, `specs/`, `legacy/`, `tests/`, audit reports, baseline tests, doc française `HOW_IT_WORKS.md`, etc.). Le kit final ne contient que ce qui est utile pour le déploiement prod.
- Découverte du bug `opendis 1.0` ne contient pas AggregateStatePdu → build custom décrit ci-dessus.
- Création de **2 guides perso** sur le bureau du user (en français) :
  - `GUIDE-INSTALL-PROD.html` (joli, à imprimer en PDF)
  - `GUIDE-INSTALL-PROD-PERSO.md` (markdown brut)

### 2026-04-23
- **PR #7 mergée** (`fix: runtime bugs discovered by /smoke-test`) :
  - Fix log spam `TimeoutError` UDP idle (1 ERROR/s avant fix)
  - Fix partitioning Kafka `pdu_type % N` qui routait tous les FirePdus sur 1 seul consumer
  - Débit mesuré post-fix : **935 msg/s** end-to-end sur le laptop (vs 258 pré-fix, x3.6)
- **Premier smoke test runtime** end-to-end : 10 000 PDUs, zéro perte, mais révélé que les 4 consumers ne se partagent pas le travail.
- **PR #5 mergée** (`fix: correctifs audit 2026-04-23`) :
  - Ajout `LoggerSQLExporter.close()` — méthode de cleanup
  - Fix shutdown hang de `kafka_main.py` (Timer threads non-daemon)
  - Fix dead code branch `EntityLocations` accumulation
  - Fix parsing `tracked_tables` vide (`['']` → `[]`)
  - Module docstrings ajoutés
- **Skill `/smoke-test`** ajouté + intégré dans `/ship` comme phase 6/9.
- **Audit hardening** (section 3.5 dans `/audit`) avec patterns runtime statiques (TimeoutError, partitioning).

### 2026-04-16
- PR #1 (audit corrections initiales) puis PR #3 (NameError, SQL_SERVER centralisé, exc_info).
- Mise en place pipeline `/ship` (9 phases) avec sous-agents `developer` et `reviewer` cloisonné.

---

## 5. Où vivent les choses

| Quoi | Où |
|---|---|
| **Code projet (dev)** | `C:\Users\hadda\Desktop\WiresharkLogger\` |
| **GUI launcher (dev)** | `C:\Users\hadda\Desktop\WiresharkLogger\launcher.py` (Tkinter NON, PyQt5) |
| **Presets launcher** | `C:\Users\hadda\Desktop\WiresharkLogger\LauncherPresets.json` (gitignoré) |
| **Repo GitHub** | https://github.com/Noamhaddad10/DataExporter |
| **venv dev** | `C:\Users\hadda\Desktop\WiresharkLogger\.venv\` (Python 3.10.11) |
| **Kafka local dev** | `C:\kafka\` (port 9092) |
| **Kafka local TEST** | `C:\kafka-test\` (port 9192, instance dédiée pour smoke tests) |
| **Transfer kit (prêt USB)** | `C:\DataExporter-prod-test\transfer-kit\` (~355 MB) |
| **Source opendis custom** | `C:\DataExporter-prod-test\opendis-source\open-dis-python-9911bcf9...` |
| **Install-test (offline simulation)** | `C:\DataExporter-prod-test\install-test-clean\` |
| **Guides perso (FR)** | `C:\Users\hadda\Desktop\GUIDE-INSTALL-PROD.html` + `.md` |
| **Mémoire Claude perso** | `C:\Users\hadda\.claude\projects\C--Users-hadda-Desktop-WiresharkLogger\memory\` |

---

## 6. Pipeline tooling Claude

Toutes les commandes Claude `/...` sont dans `.claude/commands/` :

| Commande | Rôle |
|---|---|
| `/ship "<desc>"` | Pipeline complète : issue → plan → dev → verify → smoke-test → PR → review → merge |
| `/audit [scope]` | Audit complet en lecture seule, produit `docs/audit-report-<date>.md` |
| `/smoke-test [quick\|full]` | Test runtime end-to-end (Kafka + pipeline + 10 ou 1000 PDUs) → `docs/baseline-test-<date>.md` |
| `/plan` | Plan d'implémentation pour issue courante |
| `/dev`, `/verify`, `/fix`, `/pr`, `/review`, `/merge`, `/rollback` | Étapes individuelles de la pipeline |
| `/explain` | Explication grand public de la dernière action |

Sous-agents (`.claude/agents/`) :
- `developer` : exécute le code, commit
- `reviewer` : audit isolé d'une PR (jamais de contexte du dev)

---

## 7. Bugs / quirks à connaître

### Bug Windows + Kafka chronique : "log dirs have failed"
Quand Kafka essaie de supprimer des segments physiques (`.deleted`), Windows tient parfois encore les handles ouverts (antivirus, indexer) → `AccessDeniedException` → `Shutdown broker because all log dirs have failed`.

**Mitigation** : ne jamais utiliser `kafka-topics --delete` ou nuker `kraft-logs` sans précaution. Le **fresh-start mode** (`reset_topic_on_startup: true`) évite complètement ce risque parce qu'il ne supprime rien.

**Reset nucléaire** (si vraiment bloqué) :
```powershell
Remove-Item -Recurse -Force C:\kafka\kraft-logs
$UUID = & C:\kafka\bin\windows\kafka-storage.bat random-uuid | Select-Object -Last 1
C:\kafka\bin\windows\kafka-storage.bat format --config C:\kafka\config\kraft\server.properties --cluster-id $UUID
```

### Kafka chemin court obligatoire
Kafka doit vivre dans un chemin court genre `C:\kafka\`. Chemin long → la batch script `kafka-server-start.bat` construit un classpath qui dépasse la limite Windows CMD (~8KB) → `The input line is too long`.

### Tests `tests/test_*.py` ne sont PAS pytest-compatible
Les fichiers ont des fonctions `test_*` mais avec des signatures custom (`test_X(engine, logger_file)`). Ils sont structurés comme des scripts manuels, pas comme une suite pytest. Naming trompeur. Hors scope du kit prod.

---

## 8. Open items / décisions pending

- ✅ **Build .exe PyInstaller** — DÉCIDÉ NON. Le coût (taille +200-300 MB, antivirus, multiprocessing complications, debug compliqué, rebuild à chaque modif) ne justifie pas le bénéfice (1-clic launch). À la place : `Launch DataExporter.bat` à la racine du projet (raccourci bureau possible via clic droit → Envoyer vers → Bureau).
- [ ] **Validation runtime AggregateStatePdu en prod** : à faire au premier scénario réel. Surveiller `dis.Aggregates` et `dis.AggregateLocations` qui doivent se peupler. Si elles restent à 0 alors qu'il y avait des PDUs type 33, creuser dans `dis-kafka.log`.
- [ ] **Test du shutdown gracieux PR #5** : non validé empiriquement (TaskStop = kill brutal sur Windows). À faire manuellement : `python kafka_main.py` dans console, Ctrl+C, vérifier que ça termine en < 30s avec "Pipeline stopped gracefully".
- [ ] **Décision sur perte mesurée en prod** : si jamais un scénario perd des messages (vérifié par `Σ Consumer.consumed != Producer.sent`), passer à 3 brokers Kafka + `replication-factor=3` + `min.insync.replicas=2`. Configuration documentée dans la conversation passée et le guide HTML.
- [ ] **Refacto `run_consumer`** (281 lignes, `kafka_consumer.py:45`) : signalé par audit v3/v4, pas urgent, à faire si on attaque le batching SQL plus tard.
- [ ] **Migration tests vers pytest** : signalé par audit v3/v4, pas urgent.

---

## 9. Comment je dois (Claude) maintenir ce fichier

Le user (Noam) m'a explicitement demandé de **maintenir ce fichier à jour à chaque session**.

À chaque session :
- Si on prend une **décision technique importante** → l'ajouter dans la section 4 (Décisions clés)
- Si un **bug est découvert** → mettre à jour section 7
- Si une **option de config / chemin** change → mettre à jour section 5 ou 3
- Si un **TODO se ferme** → cocher dans section 8
- Si un **nouveau TODO apparaît** → l'ajouter section 8

Mettre à jour la date "Dernière mise à jour" en haut + le commit hash de référence si applicable.

**Ne pas surcharger** : ce fichier doit rester lisible en 2-3 minutes. Si une décision est trop détaillée, la résumer ici et garder le détail dans la PR / l'audit / le guide concerné.

**Garder le ton factuel** : pas de marketing ni de "great job". État objectif du projet.
