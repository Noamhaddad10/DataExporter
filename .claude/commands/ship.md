---
description: Pipeline complète — de la description d'une tâche au merge final, avec 2 pauses utilisateur obligatoires
argument-hint: "description de la tâche en une phrase"
allowed-tools: Task, Bash, Read, Write, Edit, Glob, Grep
---

# /ship — Pipeline automatique complète

Tu orchestres toute la pipeline de développement pour une nouvelle tâche : création d'issue, planning, développement, vérification, correction, **test runtime end-to-end**, ouverture de PR, review et merge.

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

1. Annonce : `🔵 [1/9] Création de la tâche...`
2. Si la description est vague, pose 2-4 questions courtes pour cadrer
3. Vérifie `git status` clean et branche actuelle
4. Rédige le body de l'issue au format structuré (Contexte / Périmètre / Fichiers à créer ou modifier / Décisions liées / Comment vérifier / Notes)
5. Crée directement l'issue sans demander confirmation : `gh issue create`, récupère le numéro, crée `feature/<N>-<slug>`, checkout
6. Annonce : `✅ Issue #<N> créée, branche feature/<N>-<slug> checkout`

### Phase 2 — Planification (équivalent /plan)

1. Annonce : `🔵 [2/9] Réflexion sur le plan d'exécution...`
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

1. Annonce : `🔵 [3/9] Développement en cours (sous-agent developer)...`
2. Invoque `Task` avec `subagent_type: "developer"`, en passant le plan validé + les conventions projet
3. Attends la réponse du sous-agent
4. Annonce : `✅ Développement terminé — <N> commits créés`

### Phase 4 — Vérification (équivalent /verify)

1. Annonce : `🔵 [4/9] Vérification des tests et conventions...`
2. Exécute la logique de `/verify` (tests, conventions, scope, critères de succès)
3. Produis le rapport complet
4. Analyse le verdict :
   - Si `READY` → passe à la phase 6 (smoke test)
   - Si `NOT READY` → passe à la phase 5

### Phase 5 — Correction automatique (équivalent /fix), max 3 itérations

Si `/verify` a détecté des problèmes :

1. Annonce : `🔵 [5/9] Correction automatique (tentative X/3)...`
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

### Phase 6 — Smoke test runtime (équivalent /smoke-test quick)

**But** : vérifier que le pipeline tourne **pour de vrai** avant d'ouvrir la PR. L'audit statique rate parfois des bugs runtime (ordre d'exceptions, partitioning déséquilibré, log spam idle). Le smoke test les attrape.

1. Annonce : `🔵 [6/9] Smoke test runtime (pipeline réel + SQL)...`

2. **Pré-check environnement** (tout en Bash, pas d'agent) :
   - SQL Server reachable ? (connect à `localhost\SQLEXPRESS` DB `noamtest`)
   - Kafka broker reachable sur port 9092 ?
   
3. **Cas A — SQL ET Kafka up** :
   - Lance le skill `/smoke-test quick` (il gère tout : snapshot SQL, démarre `kafka_main.py`, envoie 10 PDUs, vérifie delta, arrête tout)
   - Si **verdict 🟢 OK** → continue à la phase 7
   - Si **verdict 🔴 KO** :
     ```
     ⛔ SMOKE TEST FAILED. Pipeline stoppée avant ouverture de PR.
     
     Détail : <résumé du rapport docs/baseline-test-<date>.md>
     
     Le code compile (✓ /verify) mais ne fonctionne pas end-to-end.
     Deux possibilités :
       a) Bug introduit par tes commits → /fix puis /verify puis /ship
       b) Bug pré-existant dans l'environnement (rare)
     ```
     Stop la pipeline.

4. **Cas B — SQL down OU Kafka impossible à démarrer** :
   - Tu ne skippes PAS silencieusement. Annonce :
     ```
     ⚠️ Smoke test NON exécutable (env indisponible) :
     - SQL Server : <OK|DOWN>
     - Kafka broker : <OK|DOWN>
     
     Je continue la pipeline mais le body de la PR mentionnera explicitement
     que le test runtime n'a pas pu être effectué. Classification PR probable
     dégradée d'un niveau (VERT → ORANGE).
     ```
   - Tu continues à la phase 7 **mais** tu marques `smoke_test_skipped = True` pour impacter le body PR et la classification.

5. **Cas C — utilisateur demande explicitement de sauter (variable `$SHIP_SKIP_SMOKE_TEST=1` dans l'env)** :
   - Annonce : `⚠️ Smoke test sauté sur demande utilisateur (variable SHIP_SKIP_SMOKE_TEST).`
   - Continue phase 7, marque `smoke_test_skipped = True` dans le body PR.

### Phase 7 — Ouverture de la PR (équivalent /pr)

1. Annonce : `🔵 [7/9] Ouverture de la pull request...`
2. Vérifier commits présents, branche non-main
3. Remplir automatiquement le template `.github/pull_request_template.md` avec :
   - `Closes #<N>`
   - Description issue reformulée
   - Liste des commits
   - Section "Comment tester" de l'issue
   - **Résultat du smoke test** (débit mesuré, verdict, ou "non exécuté" + raison)
   - Checklist cochée selon le diff
4. **Pas de pause** : on ouvre directement la PR avec le body pré-rempli
5. `git push -u origin <branche>` puis `gh pr create`
6. Annonce : `✅ PR #<M> ouverte : <url>`

### Phase 8 — Review stricte (équivalent /review)

1. Annonce : `🔵 [8/9] Review stricte (sous-agent reviewer isolé)...`
2. Invoque `Task` avec `subagent_type: "reviewer"` avec un prompt MINIMAL (ne JAMAIS passer le plan, les commits ou les rapports /verify/fix au reviewer)
3. Récupère le rapport complet
4. Affiche-le à l'utilisateur tel quel
5. Extrais verdict (`GO` / `GO avec réserves` / `NO GO`) et classification (`VERT` / `ORANGE` / `ROUGE`)

   **Ajustement classification** : si `smoke_test_skipped = True`, la classification est dégradée d'un niveau (VERT → ORANGE, ORANGE → ROUGE). Noter explicitement dans le rapport.

### Phase 9 — Merge conditionnel (équivalent /merge)

1. Annonce : `🔵 [9/9] Décision de merge...`

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
   **Smoke test** : <OK, X msg/s | skipped, raison>
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
   Smoke test  : <OK|SKIPPED>
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
4. **Smoke test échoué = stop pipeline.** Un code qui passe /verify mais rate /smoke-test n'est pas mergeable.
5. **Smoke test sauté doit être VISIBLE** : mention dans body PR, dégradation de classification.
6. **Toujours écrire dans le journal** après un merge. Traçabilité obligatoire.
7. **Toujours classer la PR** avant décision de merge. Pas de classification = pas de merge.
8. **En cas de doute sur une étape, stoppe et demande.** Mieux vaut une pause de plus qu'un merge foireux.

## Ce que tu ne fais JAMAIS

- Passer outre la validation du plan
- Auto-merger une PR ROUGE
- Dépasser 3 tentatives de /fix
- **Sauter le smoke test silencieusement** (toujours annoncer + marquer dans le body PR)
- **Merger après un smoke test failed** (bug démontré = stop)
- Passer des infos du développement au reviewer
- Merger sans classification explicite
- Merger sans écrire au journal
- Te passer de validation pour une PR avec verdict "GO avec réserves"
