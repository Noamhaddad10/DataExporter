---
description: Kill-switch d'urgence — annule le dernier merge automatique
allowed-tools: Bash(gh:*), Bash(git:*), Read, Write
---

# /rollback — Annulation d'urgence

Tu annules le dernier merge automatique effectué par `/merge`. À n'utiliser qu'en cas de problème détecté après le merge.

Réponds toujours en français.

## Étape 1 — Confirmation explicite

**Avant TOUTE action**, demande :

```
⚠️ /rollback va annuler le dernier merge automatique.

Cela va :
- Revert le commit de merge sur main
- Rouvrir la PR correspondante
- Rouvrir l'issue associée

C'est une action importante. Confirme en tapant : "rollback confirmé"
```

**Attends exactement la phrase "rollback confirmé".** Toute autre réponse = annulation de l'opération.

## Étape 2 — Identification du dernier merge

- Lire `specs/journal.md` pour trouver la dernière entrée
- `git log main --merges --oneline -10` pour voir les derniers merges
- Identifier le commit SHA du dernier merge auto

Affiche à l'utilisateur ce que tu comptes annuler :
```
Dernier merge identifié :
  PR #<N> : <titre>
  Commit : <SHA>
  Date : <date>
  Fichiers touchés : <liste>
```

Demande une dernière confirmation : "Je procède ? (oui / non)"

## Étape 3 — Exécution du rollback

Si confirmé :

1. `git checkout main`
2. `git pull origin main` (pour être à jour)
3. `git revert -m 1 <SHA>` (ou `git revert <SHA>` si non-merge)
4. Pousser le revert : `git push origin main`
5. Rouvrir la PR : `gh pr reopen <N>` (si possible)
6. Rouvrir l'issue : `gh issue reopen <numéro_issue>`
7. Ajouter une entrée dans `specs/journal.md` :

```markdown
## <date> — ROLLBACK de PR #<N>

**Raison** : <demander à l'utilisateur brièvement>
**Commit revert** : <nouveau SHA>
**PR rouverte** : oui / non (si n'a pas pu être rouverte)
**Issue rouverte** : oui
```

## Étape 4 — Rapport final

```
✅ Rollback effectué
- Commit de merge annulé (revert poussé sur main)
- PR #<N> rouverte pour investigation
- Issue #<numéro> rouverte
- Entrée ajoutée au journal

Ton main est maintenant dans l'état d'avant le merge.

Prochaine étape suggérée :
1. Identifier pourquoi le merge a posé problème
2. Lancer /fix sur la PR rouverte, ou abandonner la PR
```

## Cas particuliers

### Si le merge n'est pas le dernier commit de main
Plusieurs commits ont été ajoutés après le merge → le revert est plus risqué. Avertis :
```
⚠️ <N> commits ont été ajoutés sur main après ce merge.
Un revert pur pourrait créer des conflits.
Je recommande d'intervenir manuellement. Voici les commits à examiner :
<liste>
```

Demande une décision explicite avant de tenter le revert.

### Si aucun merge auto trouvé dans le journal
```
Aucun merge automatique trouvé dans specs/journal.md.
Rien à rollback. Si tu veux annuler un merge manuel, fais-le directement avec git.
```

## Ce que tu ne fais JAMAIS

- Rollback sans la phrase exacte "rollback confirmé"
- Utiliser `git reset --hard` sur main (destructif)
- Force-push sur main
- Rollback en chaîne (plusieurs merges à la fois)
- Rollback sans écrire dans le journal
