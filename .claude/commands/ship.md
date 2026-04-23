---
description: Pipeline complète — de la description d'une tâche au merge final, avec 2 pauses utilisateur obligatoires
argument-hint: "description de la tâche en une phrase"
allowed-tools: Task, Bash, Read, Write, Edit, Glob, Grep
---

# /ship — Pipeline automatique complète

Tu orchestres toute la pipeline de développement pour une nouvelle tâche : création d'issue, planning, développement, vérification, correction, ouverture de PR, review et merge.

**Tu enchaînes les étapes automatiquement, SAUF à 2 moments précis où tu t'arrêtes :**
1. Validation du plan par l'utilisateur (non négociable)
2. Si la PR est classée ROUGE (non négociable)

Tout le reste s'enchaîne sans intervention.

Réponds toujours en français, avec des messages concis à chaque étape pour que l'utilisateur suive ce qui se passe.

## Description fournie par l'utilisateur

$ARGUMENTS

---

## Séquence d'orchestration

### Phase 1 — Création de la tâche (équivalent /task)

1. Annonce : `🔵 [1/8] Création de la tâche...`
2. Si la description est vague, pose 2-4 questions courtes pour cadrer
3. Vérifie `git status` clean et branche actuelle
4. Rédige le body de l'issue au format structuré (Contexte / Périmètre / Fichiers à créer ou modifier / Décisions liées / Comment vérifier / Notes)
5. Crée directement l'issue sans demander confirmation : `gh issue create`, récupère le numéro, crée `feature/<N>-<slug>`, checkout
7. Annonce : `✅ Issue #<N> créée, branche feature/<N>-<slug> checkout`

### Phase 2 — Planification (équivalent /plan)

1. Annonce : `🔵 [2/8] Réflexion sur le plan d'exécution...`
2. Lire issue complète, specs/ existantes, fichiers listés dans le scope
3. Produire le plan au format strict (résumé, étapes numérotées avec fichier/action/détail/commit, tests prévus, conventions critiques, risques, fichiers hors scope, ordre d'exécution)
4. **🛑 PAUSE OBLIGATOIRE N°1** : affiche le plan complet et demande :
   ```
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   Je suis prêt à exécuter ce plan.
   Réponds :
     go         → je fonce, tout le reste est automatique
     ajuste: .. → j'adapte et je te montre à nouveau
     stop       → on arrête
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   ```
5. N'enchaîne PAS sans `go` explicite.

### Phase 3 — Développement (équivalent /dev)

1. Annonce : `🔵 [3/8] Développement en cours (sous-agent developer)...`
2. Invoque `Task` avec `subagent_type: "developer"`, en passant le plan validé + les conventions projet
3. Attends la réponse du sous-agent
4. Annonce : `✅ Développement terminé — <N> commits créés`

### Phase 4 — Vérification (équivalent /verify)

1. Annonce : `🔵 [4/8] Vérification des tests et conventions...`
2. Exécute la logique de `/verify` (tests, conventions, scope, critères de succès)
3. Produis le rapport complet
4. Analyse le verdict :
   - Si `READY` → passe direct à la phase 6 (saute /fix)
   - Si `NOT READY` → passe à la phase 5

### Phase 5 — Correction automatique (équivalent /fix), max 3 itérations

Si `/verify` a détecté des problèmes :

1. Annonce : `🔵 [5/8] Correction automatique (tentative X/3)...`
2. Extrais les ❌ et ⚠️ du rapport
3. Invoque `Task` avec `subagent_type: "developer"`, en passant UNIQUEMENT les problèmes à corriger
4. Attends la réponse
5. Relance la logique de `/verify`
6. Si `READY` → passe à la phase 6
7. Si `NOT READY` et tentative < 3 → boucle sur la phase 5
8. Si tentative = 3 et toujours `NOT READY` :
   ```
   ⛔ 3 tentatives de correction épuisées. Arrêt de la pipeline.
   
   Problèmes restants :
   <liste>
   
   Actions possibles :
   - Intervenir manuellement sur le code, puis relancer /verify
   - Ajuster le plan via /plan et recommencer
   - Abandonner cette branche
   ```
   Stop la pipeline.

### Phase 6 — Ouverture de la PR (équivalent /pr)

1. Annonce : `🔵 [6/8] Ouverture de la pull request...`
2. Vérifier commits présents, branche non-main
3. Remplir automatiquement le template `.github/pull_request_template.md` avec :
   - `Closes #<N>`
   - Description issue reformulée
   - Liste des commits
   - Section "Comment tester" de l'issue
   - Checklist cochée selon le diff
4. **Pas de pause** : on ouvre directement la PR avec le body pré-rempli
5. `git push -u origin <branche>` puis `gh pr create`
6. Annonce : `✅ PR #<M> ouverte : <url>`

### Phase 7 — Review stricte (équivalent /review)

1. Annonce : `🔵 [7/8] Review stricte (sous-agent reviewer isolé)...`
2. Invoque `Task` avec `subagent_type: "reviewer"` avec un prompt MINIMAL (ne JAMAIS passer le plan, les commits ou les rapports /verify/fix au reviewer)
3. Récupère le rapport complet
4. Affiche-le à l'utilisateur tel quel
5. Extrais verdict (`GO` / `GO avec réserves` / `NO GO`) et classification (`VERT` / `ORANGE` / `ROUGE`)

### Phase 8 — Merge conditionnel (équivalent /merge)

1. Annonce : `🔵 [8/8] Décision de merge...`

Applique la matrice :

**NO GO** (toute classification) :
```
❌ Review NO GO. Pipeline stoppée.
Lance `/fix` pour corriger, puis `/pr` à nouveau si besoin.
Ou intervient manuellement et relance /verify.
```
Stop.

**GO avec réserves** (toute classification) :
```
⚠️ Review GO avec réserves. Points non bloquants :
<liste>

Je NE merge PAS automatiquement dans ce cas.
Tu veux merger quand même, ou corriger les réserves d'abord ? (merge / fix / stop)
```
Attends la décision.

**GO + VERT** :
Merge automatique direct.

**GO + ORANGE** :
Vérifie diff < 200 lignes et tests tous verts.
- Si oui → merge automatique.
- Sinon → demande confirmation : "PR ORANGE avec <raison>. Je merge ? (oui / non)"

**GO + ROUGE** :
```
⛔ 🛑 PAUSE OBLIGATOIRE N°2 — PR ROUGE, auto-merge INTERDIT

Cette PR touche à des zones critiques :
<raison exacte de la classification ROUGE>

Pour ta sécurité, je ne merge pas automatiquement.

✅ PR restée ouverte : <url>
📄 Rapport d'explication en français simple écrit dans :
   .claude/reports/pr-<M>-risk-report.md

Prochaines étapes pour toi :
1. Lis le rapport ci-dessus (commande : cat .claude/reports/pr-<M>-risk-report.md)
2. Demande une review à un humain compétent OU ouvre une autre session Claude sur claude.ai pour une 2e opinion
3. Quand validé, merge manuellement via GitHub
4. Si souci détecté, /rollback n'est pas applicable (rien n'a été mergé)
```
Créer le rapport `.claude/reports/pr-<M>-risk-report.md` en français simple (même format que dans /merge).
Stop la pipeline.

### Exécution du merge (cas VERT et ORANGE validé)

1. `gh pr merge <M> --squash --delete-branch`
2. `gh issue close <N>` si pas déjà fait via `Closes #N`
3. `git checkout main && git pull origin main`
4. Écrire entrée dans `specs/journal.md` :
   ```markdown
   ## <date ISO> — PR #<M> : <titre>
   
   **Issue** : #<N>
   **Classification** : <VERT|ORANGE>
   **Commits** : <liste courte>
   **Résumé** : <1-2 phrases>
   ```
5. Affiche le récap final :
   ```
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   ✅ PIPELINE TERMINÉE
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   Issue #<N>  : fermée
   PR #<M>     : mergée (squash), branche supprimée
   Journal     : entrée ajoutée
   Main        : à jour
   
   Pour comprendre ce qui a été fait, lance : /explain
   Pour annuler en urgence : /rollback
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   ```

---

## Règles cardinales

1. **Les 2 pauses sont non négociables** : validation du plan, PR ROUGE. Jamais de contournement, même si l'utilisateur insiste dans la même commande.
2. **Le sous-agent reviewer ne doit JAMAIS recevoir de contexte du développement.** Son prompt doit rester minimal.
3. **Max 3 itérations /fix.** Au-delà, stop. Ne pas tenter une 4ème.
4. **Toujours écrire dans le journal** après un merge. Traçabilité obligatoire.
5. **Toujours classer la PR** avant décision de merge. Pas de classification = pas de merge.
6. **En cas de doute sur une étape, stoppe et demande.** Mieux vaut une pause de plus qu'un merge foireux.

## Ce que tu ne fais JAMAIS

- Passer outre la validation du plan
- Auto-merger une PR ROUGE
- Dépasser 3 tentatives de /fix
- Passer des infos du développement au reviewer
- Merger sans classification explicite
- Merger sans écrire au journal
- Te passer de validation pour une PR avec verdict "GO avec réserves"
