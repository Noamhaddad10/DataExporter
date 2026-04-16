## Issue liée
<!-- Référence obligatoire — auto-remplie par /pr à partir du nom de branche -->
Closes #

## Ce que fait cette PR
<!-- Reformulation courte de l'issue -->

## Changements notables
<!-- Liste des commits, auto-remplie par /pr -->
-

## Comment tester
<!-- Copie de la section ## Comment vérifier de l'issue -->
```bash
# Commande(s) de vérification
```

---

## Checklist

**Code & Tests**
- [ ] `pytest` passe localement
- [ ] Tests ajoutés — ce qui est couvert :
    -
- [ ] Logs ajoutés sur les chemins importants (via `log = logging.getLogger(...)`)
- [ ] `exc_info=True` sur tous les `log.error()` liés à une exception

**Config & Conventions**
- [ ] Nouvelles constantes UNIQUEMENT dans `kafka_config.py` — oui / non / N/A
- [ ] Nouvelles clés dans `DataExporterConfig.json` avec défaut via `.get()` — oui / non / N/A
- [ ] Imports toujours via `import kafka_config as cfg` — aucune valeur en dur ailleurs
- [ ] `stop_event` respecté dans les boucles de processes — oui / non / N/A

**Kafka spécifique**
- [ ] `enable.auto.commit = False` respecté, commit manuel APRÈS insert SQL — oui / non / N/A
- [ ] Partitioning cohérent (`pdu_type % NUM_PARTITIONS`) — oui / non / N/A
- [ ] Format message Kafka respecté (`struct.pack("!d", packet_time) + raw_pdu`) — oui / non / N/A

**SQL Server**
- [ ] Schéma `dis` via `sql_meta = sqlalchemy.MetaData(schema="dis")` — oui / non / N/A
- [ ] Batch inserts avec `cfg.BATCH_SIZE` / `cfg.BATCH_TIME` — oui / non / N/A
- [ ] Script de création de table fourni si nouvelle table — oui / non / N/A

**Specs & Doc**
- [ ] Specs à jour si impactées — oui / non / N/A
- [ ] `specs/decisions.md` mis à jour si nouvelle décision DXXX — oui / non / N/A
- [ ] `specs/changelog.md` mis à jour — oui / non

**Gitignore**
- [ ] Aucun fichier ignoré commité (`DataExporterConfig.json`, `*.log`, `*.exe`, `triggers.sql`, `/logs/`)

**Pipeline**
- [ ] `/verify` a retourné READY avant ouverture de la PR
- [ ] Classification attendue (auto-évaluation) : VERT / ORANGE / ROUGE

**Breaking changes** <!-- Décrire si applicable -->

**Logs de test** <!-- Extrait des logs si pertinent : PDU rate, errors, stats -->
