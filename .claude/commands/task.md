---
description: Crée une issue GitHub structurée et une branche feature à partir d'une description courte
argument-hint: "description de la tâche en une phrase"
allowed-tools: Bash(gh:*), Bash(git:*)
---

# /task — Création d'une tâche structurée

Tu prends la description courte ci-dessous et tu la transformes en issue GitHub parfaitement structurée, que `/review` pourra auditer ensuite.

Tu ne codes rien. Tu ne modifies aucun fichier du projet. Tu crées une issue GitHub et une branche Git, point.

Réponds toujours en français.

## Description fournie par l'utilisateur

$ARGUMENTS

## Étape 1 — Cadrage

Si la description est vague, pose 2 à 4 questions courtes pour obtenir :
- Les **fichiers concernés** (s'ils ne sont pas évidents depuis le code)
- Les **décisions D0XX** de `specs/decisions.md` qui s'appliquent
- Les **critères de succès vérifiables** (quel test passe, quelle commande donne quel résultat)

Si la description est déjà précise, ne pose pas de questions inutiles. Passe à l'étape 2.

## Étape 2 — Lecture du repo

- `gh repo view --json name,owner,defaultBranchRef`
- `git status` (doit être clean, sinon avertir)
- `git branch --show-current`
- Lire `specs/decisions.md` s'il existe pour identifier les D0XX pertinentes
- Lire rapidement les fichiers cités dans la description pour confirmer la faisabilité

## Étape 3 — Rédaction de l'issue

Prépare le body de l'issue au format suivant exactement :

```markdown
## Contexte
<1 à 3 phrases expliquant pourquoi cette tâche existe>

## Périmètre
<Liste à puces des fichiers qui SERONT modifiés ou créés. Un fichier par ligne.>

## Fichiers à créer / modifier
- `chemin/fichier1.py` — <nature du changement>
- `chemin/fichier2.py` — <nature du changement>

## Décisions liées
<Liste des D0XX applicables, ou "Aucune" si rien de pertinent>

## Comment vérifier
<Instructions EXACTES pour valider que la tâche est réussie. Commande + résultat attendu.>
- [ ] Critère 1
- [ ] Critère 2

## Notes
<Optionnel : risques, dépendances, références externes>
```

## Étape 4 — Validation utilisateur

Affiche le body complet de l'issue et demande :
"Voici l'issue que je vais créer. Tu veux que je la crée telle quelle, que je l'ajuste, ou qu'on arrête ? (créer / ajuste: <ton commentaire> / stop)"

**Attends explicitement la réponse.** Ne crée pas l'issue sans validation.

## Étape 5 — Création

Une fois validé :
1. Crée l'issue : `gh issue create --title "<titre court>" --body "<body>"`
2. Récupère le numéro retourné (ex: `#47`)
3. Calcule un slug kebab-case à partir du titre (max 4 mots significatifs, en anglais)
4. Crée et checkout la branche : `git checkout -b feature/<numéro>-<slug>`
5. Affiche le récap :
   ```
   ✅ Issue #<N> créée : <url>
   ✅ Branche feature/<N>-<slug> checkout
   ✅ Prêt pour /plan
   ```

## Ce que tu ne fais JAMAIS

- Créer l'issue sans validation utilisateur
- Écrire du code ou modifier des fichiers du projet
- Checkout sur `main` ou `master`
- Créer une branche si le repo n'est pas propre (`git status` doit être clean)
