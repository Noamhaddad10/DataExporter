# Comment marche la pipeline — Guide d'utilisation

Ce document explique en français simple comment utiliser la pipeline de développement automatique de ton projet Data Exporter.

**À qui ça s'adresse :** toi, qui as mis en place cette pipeline mais qui n'est pas à l'aise avec tous les détails techniques du code. Pas de jargon inutile ici.

---

## Résumé en 30 secondes

Tu as **une seule commande** à retenir : `/ship`.

Tu l'écris suivi d'une phrase qui décrit ce que tu veux faire. Claude s'occupe de **tout le reste** : il crée une issue GitHub, réfléchit à comment faire, code, teste, se relit lui-même, ouvre une pull request, l'audite, et la merge si tout est bon.

Tu interviens **2 fois seulement** :
1. Pour valider le plan avant qu'il code
2. Si le changement est classé "ROUGE" (zone critique), tu dois faire valider par un humain avant le merge

Le reste, c'est automatique.

---

## Le flux complet, étape par étape

### Étape 1 — Tu lances `/ship`

```
/ship "ajouter le support du PduType 33 EventReportPdu"
```

Ta phrase doit être courte et claire. Si elle est trop vague, Claude te posera 2-3 questions pour cadrer.

### Étape 2 — Claude crée une issue GitHub

Claude rédige une issue bien structurée : de quoi il s'agit, quels fichiers sont touchés, comment on va vérifier que ça marche.

Il te montre l'issue et te demande : "oui, ajuste, ou stop ?" Tu dis `oui`. L'issue est créée avec un numéro (ex: #47) et une branche Git `feature/47-event-report-pdu` est checkout.

### Étape 3 — Claude produit un plan

Claude réfléchit à voix haute :
- Quels fichiers il va toucher
- Dans quel ordre
- Quels tests il va écrire
- Les risques qu'il voit

**⏸ PAUSE 1 — C'est ici que tu interviens.**

Tu vois le plan s'afficher avec une question :
```
go         → je fonce, tout le reste est automatique
ajuste: .. → j'adapte et je te montre à nouveau
stop       → on arrête
```

Si tu tapes `go`, c'est parti. Tu ne touches plus à rien jusqu'à la fin (sauf cas ROUGE).

### Étape 4 — Claude développe

Un sous-agent "developer" prend le relais. Il code, il respecte les conventions du projet, il commit au fur et à mesure.

Tu vois juste : `🔵 [3/8] Développement en cours...` puis `✅ Développement terminé — 4 commits créés`.

### Étape 5 — Claude vérifie son travail

Il lance les tests, vérifie que les conventions sont respectées, compare ce qu'il a fait à ce que demande l'issue.

Il produit un rapport avec des ✅ / ⚠️ / ❌.

### Étape 6 — Si problèmes, Claude se corrige

Si le rapport contient des ❌ ou ⚠️, Claude repart corriger **uniquement ces points**, sans toucher au reste.

Il peut essayer jusqu'à **3 fois**. Si au bout de 3 tentatives ça ne marche toujours pas, il s'arrête et te demande de l'aide.

### Étape 7 — Claude ouvre une pull request

Quand tout passe, Claude pousse la branche sur GitHub et ouvre une pull request avec une description bien remplie.

### Étape 8 — Un autre Claude relit

**C'est le moment le plus important à comprendre.**

Un deuxième sous-agent, le "reviewer", démarre. Il n'a **jamais vu le plan**, il n'a **jamais vu le développement**, il n'a **jamais vu les corrections**. Il lit juste :
- L'issue GitHub
- Le résultat final (le diff)

C'est comme demander à un collègue qui arrive frais de relire ton travail. Il ne peut pas se rationaliser en disant "ah oui je me souviens pourquoi j'ai fait ça". Il ne se souvient de rien.

Le reviewer produit un verdict :
- **GO** : tout est bon
- **GO avec réserves** : des petits trucs non bloquants
- **NO GO** : il y a un problème

Et une classification :
- **VERT** : changement mineur, sans danger (tests, logs, typos)
- **ORANGE** : changement normal, moins de 200 lignes, pas de zone critique
- **ROUGE** : changement sensible (schéma SQL, `kafka_main.py`, nouvelle dépendance, plus de 200 lignes)

### Étape 9 — Claude décide du merge

Matrice de décision :

| Verdict | Classification | Ce qui se passe |
|---|---|---|
| NO GO | peu importe | Pipeline stoppée, lance `/fix` |
| GO avec réserves | peu importe | Claude te demande si tu veux merger quand même |
| GO | VERT | Auto-merge immédiat ✅ |
| GO | ORANGE | Auto-merge si petit, sinon confirmation |
| GO | ROUGE | **⏸ PAUSE 2** — Auto-merge interdit |

### Étape 10 — Cas du PR ROUGE (ta 2ème intervention possible)

Si la PR est classée ROUGE, Claude s'arrête et t'écrit un **rapport en français simple** dans `.claude/reports/pr-<N>-risk-report.md`.

Ce rapport t'explique :
- Ce qui a changé, en mots simples
- Pourquoi c'est classé ROUGE
- Qui tu peux demander pour une review humaine
- Comment tester manuellement

Tu lis le rapport. Si ça t'inquiète, tu demandes de l'aide (collègue, forum, autre session Claude sur claude.ai pour une 2e opinion). Si c'est OK pour toi, tu merges manuellement via GitHub.

### Étape 11 — Après le merge

Claude met à jour `specs/journal.md` avec une entrée expliquant ce qui s'est passé. Ce journal est ta source de vérité pour comprendre l'historique du projet.

---

## Les commandes disponibles

### La principale (95% du temps)

- **`/ship "description"`** — pipeline complète, c'est tout

### Les commandes de secours (5% du temps)

- **`/explain`** — à tout moment, tu tapes ça et Claude t'explique en français simple ce qui vient de se passer. Utile quand tu veux comprendre sans pression.
- **`/rollback`** — kill-switch d'urgence. Si un merge auto a causé un problème, ça l'annule.

### Les commandes individuelles (pour plus tard, quand tu maîtriseras)

Elles font chacune une étape de `/ship`. Utile si tu veux reprendre une pipeline interrompue, ou tester une étape isolée.

- `/task` — juste créer l'issue et la branche
- `/plan` — juste réfléchir
- `/dev` — juste coder (exécuter un plan validé)
- `/verify` — juste vérifier
- `/fix` — juste corriger
- `/pr` — juste ouvrir la PR
- `/review` — juste reviewer
- `/merge` — juste décider du merge

---

## Exemples concrets

### Exemple 1 — Ajouter une fonctionnalité simple

```
> /ship "ajouter un log INFO dans kafka_consumer quand on traite un EntityStatePdu"

[Claude pose peut-être 1-2 questions]
[Claude crée l'issue #51]
[Claude produit un plan court]

Plan :
  1. Ajouter log dans kafka_consumer.py ligne ~180
  Commit prévu : `feat: add INFO log on EntityStatePdu processing`
  Tests : mise à jour test_kafka_consumer.py pour vérifier l'appel au log

go / ajuste: / stop ?

> go

[Claude code, teste, ouvre PR, reviewe]

Verdict : GO
Classification : VERT
→ Auto-merge ✅

Issue #51 fermée. PR #52 mergée. Tu es sur main à jour.
```

Durée totale : 2-3 minutes. Tu as tapé `/ship` et `go`.

### Exemple 2 — Changement risqué

```
> /ship "changer la stratégie de partitioning pour utiliser site_id au lieu de pdu_type"

[...]

Verdict : GO
Classification : ROUGE (changement de la stratégie de partitioning, affecte kafka_producer.py et décision D003)

⛔ Auto-merge INTERDIT.

Rapport écrit dans .claude/reports/pr-48-risk-report.md

Prochaines étapes :
1. Lis le rapport
2. Demande validation humaine
3. Merge manuellement via GitHub si OK
```

Tu ouvres le rapport, tu lis l'explication simple, tu fais valider. Tu gardes le contrôle.

### Exemple 3 — Un bug détecté en prod après un merge auto

Tu remarques que le pipeline plante depuis hier.

```
> /rollback

⚠️ /rollback va annuler le dernier merge automatique.
Cela va :
- Revert le commit de merge sur main
- Rouvrir la PR correspondante
- Rouvrir l'issue associée

Confirme en tapant : "rollback confirmé"

> rollback confirmé

[Claude identifie le dernier merge, demande confirmation finale]
[Claude revert le commit, rouvre PR et issue]

✅ Rollback effectué. Main est revenu à l'état d'avant le merge.
```

---

## Les règles que Claude ne contournera jamais

1. **Il ne code pas sans validation du plan** — la 1ère pause est obligatoire
2. **Il ne merge jamais une PR ROUGE automatiquement** — la 2ème pause est obligatoire
3. **Il limite `/fix` à 3 tentatives** — au-delà il s'arrête et demande
4. **Le reviewer n'a jamais accès au contexte du développeur** — isolation stricte
5. **Il écrit toujours dans le journal après un merge** — traçabilité garantie

Ces règles sont là pour te protéger. Ne demande pas à Claude de les contourner.

---

## Que faire si tu es perdu

À n'importe quel moment, tape `/explain`. Claude te dira en français simple ce qu'il vient de faire.

Si tu veux comprendre un concept précis :
```
/explain "pourquoi la PR est ROUGE"
/explain "ce que fait kafka_producer"
/explain "stop_event"
```

Il te répondra avec des analogies, pas du jargon.

---

## Le conseil que je te donne

Cette pipeline va faire beaucoup de choses toute seule. C'est **pratique** mais aussi **dangereux** si tu ne regardes jamais ce qui se passe.

Mon conseil honnête : **une fois de temps en temps** (une fois par semaine, par quinzaine, à ton rythme), ouvre `specs/journal.md` et lis les 2-3 dernières entrées. Si tu comprends pas un truc, tape `/explain "<le truc en question>"`.

Tu apprendras à ton rythme, sans effort, juste en étant curieux. Dans 6 mois, tu sauras beaucoup mieux ce qu'il y a dans ton projet. Et ça t'aidera aussi dans tes études (Bagrut CS, puis Reichman).

Mais c'est une **recommandation**, pas une obligation. La pipeline marche même si tu ne lis jamais le journal.
