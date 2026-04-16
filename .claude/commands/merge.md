---
description: Merge la PR si les conditions d'auto-merge sont réunies, sinon bloque et produit un rapport
allowed-tools: Bash(gh:*), Bash(git:*), Read, Write
---

# /merge — Auto-merge conditionnel

Tu décides de merger ou pas, en fonction du verdict et de la classification produits par `/review`.

Réponds toujours en français.

## Étape 1 — Récupération du contexte

Cherche dans la conversation le dernier rapport `/review` et extrais :
- Verdict : `GO` / `GO avec réserves` / `NO GO`
- Classification : `VERT` / `ORANGE` / `ROUGE`

Si pas de rapport `/review` trouvé : dis "Lance /review d'abord." et stoppe.

## Étape 2 — Vérifications techniques

- `gh pr view --json number,mergeable,mergeStateStatus,statusCheckRollup`
- La PR doit être `mergeable: MERGEABLE`
- Les checks CI (s'il y en a) doivent être au vert

Si un check échoue : bloque et affiche l'erreur.

## Étape 3 — Décision selon la matrice

| Verdict | Classification | Décision |
|---|---|---|
| NO GO | n'importe | ❌ Ne merge pas. Suggère `/fix`. |
| GO avec réserves | n'importe | ⏸  Demande confirmation à l'utilisateur. |
| GO | VERT | ✅ Auto-merge. |
| GO | ORANGE | ✅ Auto-merge (si diff < 200 lignes, si tous tests verts). Sinon ⏸ . |
| GO | ROUGE | ⛔ Auto-merge INTERDIT. Produit un rapport. |

### Cas NO GO
```
❌ Review NO GO. Merge impossible.
Lance /fix pour corriger les problèmes identifiés.
```

### Cas GO avec réserves
```
⚠️ Review GO avec réserves. Voici les points non bloquants :
<liste>

Tu veux que je merge quand même ? (oui / non / fix)
```

### Cas GO + VERT
Procède directement au merge (étape 4).

### Cas GO + ORANGE
Vérifie :
- `git diff main...HEAD --shortstat` → lignes ajoutées/supprimées
- Si total < 200 lignes et tous tests verts → merge (étape 4)
- Sinon → demande confirmation à l'utilisateur

### Cas GO + ROUGE
```
⛔ PR classée ROUGE — auto-merge INTERDIT

Cette PR touche à des zones critiques :
<raison exacte de la classification ROUGE>

Pour ta sécurité, je ne peux pas la merger automatiquement. Une review humaine est nécessaire.

J'ai écrit un rapport détaillé pour toi dans :
.claude/reports/pr-<N>-risk-report.md

Ce rapport explique en français simple :
- Ce qui a changé
- Pourquoi c'est classé ROUGE
- Qui tu peux demander pour une review (collègue senior, forum Stack Overflow, autre session Claude sur .ai)

La PR reste ouverte. Tu peux la merger manuellement via GitHub une fois validée.
```

**Créer le rapport** `.claude/reports/pr-<N>-risk-report.md` :

```markdown
# Rapport de risque — PR #<N>

## Titre de la PR
<titre>

## Classification : ROUGE

## Raison de la classification
<raison exacte extraite du rapport /review>

## Ce qui a changé (en français simple)

<Reformuler les changements du diff en termes non-techniques quand possible>

### Fichiers modifiés
- `<fichier>` — <description simple>

### Changements clés
<Paragraphes courts décrivant les changements importants sans jargon>

## Pourquoi c'est risqué

<Explication en français simple du risque. Ex: "Le fichier kafka_main.py est le chef d'orchestre de tout le pipeline. Une erreur ici peut empêcher tout le système de démarrer.">

## Ce que tu peux faire

1. **Demander une review humaine** : montre cette PR à un collègue qui connaît le projet
2. **Poster une question** sur un forum technique (Stack Overflow) avec le diff
3. **Ouvrir une autre session Claude** sur claude.ai, coller le diff et demander une seconde opinion
4. **Tester manuellement** en local avant de merger : <commandes de test extraites de l'issue>

## Recommandation

Ne merge pas avant d'avoir au moins UNE validation externe.
```

## Étape 4 — Exécution du merge (si autorisé)

1. `gh pr merge <N> --squash --delete-branch`
2. `gh issue close <numéro_issue>` (si pas déjà fermée via Closes #N)
3. `git checkout main`
4. `git pull origin main`
5. Écrire dans `specs/journal.md` (créer si absent) une entrée :

```markdown
## <date> — PR #<N> : <titre>

**Issue** : #<numéro issue>
**Classification** : <VERT|ORANGE>
**Commits** : <liste courte>
**Résumé** : <1-2 phrases>
```

Affiche :
```
✅ PR #<N> mergée en squash
✅ Branche supprimée
✅ Issue #<numéro issue> fermée
✅ Entrée ajoutée à specs/journal.md

Tu es de retour sur main, à jour.
```

## Ce que tu ne fais JAMAIS

- Merger une PR ROUGE automatiquement, même si l'utilisateur insiste dans la même commande
- Force-merger via `--admin`
- Ignorer un check CI rouge
- Merger sans rapport /review valide dans la conversation
- Sauter l'écriture du journal
