---
description: Lit l'issue courante et produit un plan d'exécution détaillé SANS ÉCRIRE DE CODE
allowed-tools: Bash(gh:*), Bash(git:*), Read, Glob, Grep
---

# /plan — Réflexion avant action

Tu réfléchis à comment implémenter la tâche, tu produis un plan détaillé, et tu attends validation.

**Règle absolue : tu n'écris AUCUN CODE, tu ne modifies AUCUN FICHIER à ce stade.** Même pas un test. Même pas une ligne. Ton seul livrable est un plan en texte.

Réponds toujours en français.

## Étape 1 — Contexte

- `git branch --show-current` (doit ressembler à `feature/<N>-<slug>`, sinon avertir)
- Extraire le numéro d'issue depuis le nom de la branche
- `gh issue view <N> --json body,title,labels`
- Extraire : `## Périmètre`, `## Fichiers à créer / modifier`, `## Décisions liées`, `## Comment vérifier`

## Étape 2 — Lecture de l'existant

- Lire `specs/decisions.md`, `specs/technical/*.md`, `specs/functional/*.md` s'ils existent
- Lire EN ENTIER chaque fichier listé dans `## Fichiers à créer / modifier`
- Identifier les fonctions, classes, constantes existantes qui seront touchées ou servent de référence

## Étape 3 — Production du plan

Format du plan — strict :

```
# Plan d'exécution — Issue #<N> : <titre>

## Résumé en une phrase
<Ce qu'on va faire, vu de très haut>

## Étapes d'implémentation

### Étape 1 : <nom court>
- **Fichier** : `chemin/fichier.py`
- **Action** : <créer | modifier | supprimer>
- **Détail** : <ce qu'on ajoute ou change, en 2-3 lignes>
- **Lignes concernées** : <ligne X, après la fonction Y, etc.>
- **Commit prévu** : `<type>: <message>`

### Étape 2 : <nom court>
...

## Tests prévus

### Test 1 : <nom>
- **Fichier** : `tests/test_xxx.py`
- **Ce qu'il vérifie** : <description>
- **Mock utilisés** : <Kafka broker / SQL / aucun>

## Conventions critiques à respecter

<Liste courte des conventions du projet qui s'appliquent particulièrement ici, ex :
- kafka_config.py : nouvelle constante PDU_TYPE_EVENT_REPORT avec type hint et commentaire
- logging : getLogger("event_report_handler"), exc_info=True sur erreurs
- stop_event respecté dans la nouvelle boucle>

## Risques identifiés

<Liste courte des choses qui peuvent mal tourner, ex :
- Si NUM_PARTITIONS change avant déploiement, la clé de partition doit être recalculée
- L'insertion SQL peut timeout si le batch est trop gros
- Aucun risque identifié>

## Fichiers touchés hors scope de l'issue

<Si on a détecté qu'il faudra toucher un fichier non listé dans l'issue, on le signale ICI. Si rien : "Aucun.">

## Ordre d'exécution

1. <étape X>
2. <étape Y>
3. <étape Z>
4. <tests>

Estimation : <N> commits au total.
```

## Étape 4 — Demande de validation

Affiche le plan complet, puis :

```
Tu veux que je lance /dev pour exécuter ce plan ?

Réponses possibles :
  go           → j'exécute le plan tel quel
  ajuste: ...  → j'adapte selon tes remarques et je te représente le plan
  stop         → on arrête là
```

**Attends la réponse explicite. N'enchaîne JAMAIS sur l'exécution sans un `go` explicite.**

## Si l'utilisateur dit "go"

Réponds simplement : "Plan validé. Lance `/dev` pour exécuter."

**Tu ne lances PAS `/dev` toi-même.** L'utilisateur (ou `/ship`) s'en charge. Ton rôle s'arrête ici.

## Si l'utilisateur dit "ajuste: ..."

Révise le plan en tenant compte des remarques et représente-le depuis l'étape 3. Boucle jusqu'à `go` ou `stop`.

## Si l'utilisateur dit "stop"

Réponds : "Plan abandonné. Aucune modification effectuée. Tu peux relancer /plan ou /task selon tes besoins."

## Ce que tu ne fais JAMAIS

- Écrire, créer, modifier ou supprimer un fichier
- Lancer `/dev` ou une autre commande
- Commencer l'implémentation "juste pour voir"
- Valider le plan toi-même
