---
description: Vérifie le travail effectué (tests, conventions, scope) sans rien corriger. Produit un rapport.
allowed-tools: Bash, Read, Glob, Grep
---

# /verify — Diagnostic

Tu vérifies que le travail effectué correspond à l'issue et respecte les conventions. Tu ne corriges RIEN. Tu produis un rapport.

Réponds toujours en français.

## Étape 1 — Contexte

- `git branch --show-current` et extraire `<N>` du nom de branche
- `gh issue view <N> --json body,title`
- Extraire scope, décisions, critères de succès
- `git diff main...HEAD` pour voir tous les changements de la branche

## Étape 2 — Tests

Si des tests existent :
- `pytest -v 2>&1` et capturer le résultat
- Compter passed/failed/errors
- Identifier les tests liés au diff (même nom de module)

Si aucun test pour les fichiers du diff : le signaler en ⚠️.

## Étape 3 — Vérifications de conventions

Pour chaque fichier du diff, vérifier selon sa nature :

### `kafka_config.py`
- [ ] Nouvelles constantes ont un type hint
- [ ] Nouvelles constantes lues depuis `_RAW` avec `.get()` + défaut
- [ ] Commentaire d'une ligne par nouvelle constante
- [ ] Aucune constante Kafka/DIS dupliquée ailleurs

Commande utile : `grep -n "=" fichier.py` pour vérifier le pattern.

### `kafka_producer.py` / `kafka_consumer.py`
- [ ] `import kafka_config as cfg` présent
- [ ] `_configure_worker_logger(log_queue)` en début de process
- [ ] Aucun `print()` sauf les PID de démarrage existants (`grep -n "print(" fichier.py`)
- [ ] `exc_info=True` sur tous les `log.error()` récents (`grep -n "log.error\|logger.error" fichier.py`)
- [ ] `while not stop_event.is_set()` dans toutes les nouvelles boucles
- [ ] Pas de `except Exception:` nu (`grep -n "except Exception" fichier.py`)

### `kafka_main.py`
- [ ] Format de log `"%(asctime)s | %(processName)s..."` préservé
- [ ] `stop_event` et `log_queue` passés aux nouveaux processes
- [ ] `flush()` et `join()` dans la séquence de shutdown

### `LoggerSQLExporter.py` / `LoggerPduProcessor.py`
- [ ] Schéma `dis` utilisé (`grep -n "schema" fichier.py`)
- [ ] Connexion SQL en paramètre, pas instanciée
- [ ] Batch avec `cfg.BATCH_SIZE` / `cfg.BATCH_TIME`

### Tous fichiers Python
- [ ] Aucun import inutile (difficile à vérifier mécaniquement, signaler si suspect)
- [ ] Aucune nouvelle dépendance sans mise à jour de `requirements.txt`
- [ ] Aucun fichier du `.gitignore` commité : vérifier `git diff --name-only main...HEAD` vs `.gitignore`

## Étape 4 — Vérification du scope

Comparer `git diff --name-only main...HEAD` avec les fichiers listés dans `## Périmètre` et `## Fichiers à créer / modifier` de l'issue :
- Fichier attendu absent → ❌ CRITIQUE
- Fichier présent non attendu → ⚠️ EXTRAPOLATION

## Étape 5 — Critères de succès

Pour chaque checkbox dans `## Comment vérifier` de l'issue :
- Si c'est une commande : la lancer si possible et comparer au résultat attendu
- Si c'est un comportement externe (test d'intégration) : signaler comme non-vérifiable automatiquement et demander une vérification manuelle

## Étape 6 — Rapport

Format strict :

```
# Rapport /verify — Issue #<N>

## Tests
<résultat pytest>
Verdict tests : ✅ / ⚠️ / ❌

## Conventions
- ✅/⚠️/❌ <convention> : <fichier:ligne si problème>
- ...

## Scope
- Fichiers attendus manquants : <liste ou "aucun">
- Fichiers hors scope : <liste ou "aucun">

## Critères de succès de l'issue
- ✅/⚠️/❌ <critère 1>
- ✅/⚠️/❌ <critère 2>

## Synthèse

Nombre de ❌ : <N>
Nombre de ⚠️ : <N>
Nombre de ✅ : <N>

Verdict : <READY / NOT READY>

<Si NOT READY : liste ordonnée des points à corriger, la plus actionnable possible>

Action suggérée : <"/pr" si READY, "/fix" si NOT READY>
```

## Ce que tu ne fais JAMAIS

- Modifier un fichier
- Corriger un problème
- Écrire du code
- Commit quoi que ce soit
- Être indulgent : un ⚠️ reste un ⚠️, même si "c'est pas grave"
