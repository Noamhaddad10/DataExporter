---
description: Push la branche et ouvre une pull request avec le template rempli automatiquement
allowed-tools: Bash(gh:*), Bash(git:*), Read
---

# /pr — Ouverture de la pull request

Tu push la branche courante et tu ouvres une pull request bien remplie.

Réponds toujours en français.

## Étape 1 — Vérifications préalables

- `git branch --show-current` (doit être `feature/<N>-<slug>`, pas `main`)
- `git status` (doit être clean)
- `git log main..HEAD --oneline` (doit avoir au moins 1 commit)

Si une condition échoue, stoppe et affiche le problème.

## Étape 2 — Récupérer l'issue

- Extraire `<N>` du nom de branche
- `gh issue view <N> --json body,title,url`

## Étape 3 — Lire le template PR

- `cat .github/pull_request_template.md` (doit exister)
- Si absent : avertir, utiliser un template minimal inline

## Étape 4 — Remplir le template

Remplis le template automatiquement à partir de :
- **Issue liée** : `Closes #<N>`
- **Ce que fait cette PR** : issue.title + 1-2 phrases de reformulation
- **Changements notables** : extraction des commits `git log main..HEAD --format="- %s"`
- **Comment tester** : copier la section `## Comment vérifier` de l'issue
- **Checklist** : cocher automatiquement ce qui est vrai (tests présents, pas de `print()`, etc.) en se basant sur le diff. Laisser à `[ ]` ce qui doit être validé manuellement.
- **Breaking changes** : laisser vide ou "Aucun" si rien détecté
- **Logs de test** : si `/verify` a été lancé dans la conversation, coller un extrait

## Étape 5 — Validation du body

Affiche le body complet de la PR et demande :
```
Voici la description de la PR que je vais ouvrir. OK ?
  ouvre       → je push et j'ouvre la PR
  ajuste: ... → j'adapte selon tes remarques
  stop        → on arrête
```

**Attends la réponse.**

## Étape 6 — Push et ouverture

Une fois validé :
1. `git push -u origin <branche>`
2. `gh pr create --title "<titre issue>" --body "<body>" --base main`
3. Récupère l'URL de la PR

Affiche :
```
✅ Branche pushée
✅ PR ouverte : <url>

Prochaine étape : /review
```

## Ce que tu ne fais JAMAIS

- Push sur `main`
- Force-push
- Modifier des fichiers du projet (sauf remplissage de template)
- Ouvrir une PR sans validation utilisateur du body
- Lancer `/review` ou `/merge` tout seul
