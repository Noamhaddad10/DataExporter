# specs/ — Documentation du projet Data Exporter

Ce dossier contient la documentation technique et fonctionnelle du projet **Data Exporter** (pipeline DIS → Kafka → SQL Server).

Règle d'or : **si un changement de code modifie le comportement, l'architecture ou les décisions du projet, la doc correspondante doit être mise à jour dans la même PR.**

---

## Structure du dossier

```
specs/
├── README.md                         ← ce fichier
├── journal.md                        ← historique des merges automatiques (auto-rempli par /merge)
├── functional/
│   └── dis-pipeline.md               ← ce que fait le data exporter (côté métier)
├── technical/
│   ├── kafka-architecture.md         ← fonctionnement du pipeline Kafka
│   └── sql-schema.md                 ← tables SQL, schéma dis
├── decisions.md                      ← choix d'architecture (D001, D002...)
├── roadmap.md                        ← ce qui est prévu
├── known-issues.md                   ← bugs connus non résolus
└── changelog.md                      ← historique des changements majeurs
```

---

## À quoi sert chaque fichier

### `functional/dis-pipeline.md`
**Le QUOI.** Décrit ce que fait le pipeline du point de vue d'un utilisateur : quels PDUs sont acceptés, quel port UDP, où finissent les données.

### `technical/kafka-architecture.md`
**Le COMMENT côté Kafka.** Décrit les processes (`kafka_producer`, `kafka_consumer`, `kafka_main`), le partitioning, le format du message, la gestion des offsets.

### `technical/sql-schema.md`
**Le COMMENT côté SQL.** Décrit les tables du schéma `dis`, leurs colonnes, la logique de batch insert.

### `decisions.md`
**Le POURQUOI.** Format ADR court :

```markdown
## D001 — Choix de Kafka plutôt que SharedMemory RingBuffer

**Date :** 2026-04-14
**Contexte :** <problème résolu>
**Décision :** <choix fait>
**Conséquence :** <impact>
```

Les décisions sont numérotées et **ne sont jamais modifiées** après écriture. Si une décision est remplacée, on écrit une nouvelle qui référence l'ancienne.

### `journal.md`
**L'historique des merges automatiques.** Rempli automatiquement par `/merge` à chaque merge réussi. Chaque entrée contient :
- Date, numéro de PR, issue liée
- Classification (VERT ou ORANGE)
- Commits, résumé en 1-2 phrases

Ce fichier est **ton outil principal pour comprendre ce que la pipeline a fait au fil du temps**, même si tu ne lis pas le code. Si tu veux apprendre à ton rythme, lis une entrée de temps en temps et pose des questions à Claude.

### `roadmap.md`
Liste courte des prochaines étapes.

### `known-issues.md`
Bugs connus non résolus ou limitations assumées.

### `changelog.md`
Format [Keep a Changelog](https://keepachangelog.com/fr/1.1.0/) simplifié.

---

## Règles de rédaction

- **Français technique** : on garde les termes du code (`producer`, `consumer`, `partition`, `PDU`, `stop_event`)
- **Phrases courtes** : une idée par phrase
- **Pas de marketing** : pas de "robuste", "performant", "élégant"
- **Toujours citer les fichiers** : quand on décrit un comportement, on cite fichier et fonction
- **Diagrammes ASCII** bienvenus quand ça clarifie

Le dossier démarre quasi vide. Les fichiers se remplissent naturellement au fil des PRs, quand `/merge` détecte qu'une décision d'architecture a été prise.
