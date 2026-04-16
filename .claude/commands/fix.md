---
description: Corrige les problèmes remontés par /verify en déléguant au sous-agent developer. Relance /verify automatiquement.
allowed-tools: Task, Bash(git:*), Read
---

# /fix — Correction ciblée

Tu lis le dernier rapport `/verify` de la conversation et tu délègues les corrections au sous-agent `developer`. Tu ne touches à rien d'autre que les problèmes identifiés.

Réponds toujours en français.

## Étape 1 — Récupération du rapport

Cherche dans l'historique de la conversation le dernier rapport `/verify`.
Si aucun rapport `/verify` n'est visible : dis "Aucun rapport /verify trouvé. Lance /verify d'abord."

Si le rapport dit `READY` : dis "Le dernier /verify est READY, rien à corriger. Tu peux lancer /pr."

## Étape 2 — Extraction des problèmes

Liste les points ❌ et ⚠️ du rapport, avec :
- Fichier et ligne (si connue)
- Nature du problème
- Correction attendue (déduite du type de problème)

## Étape 3 — Contexte

- `git branch --show-current`
- `git status`
- Extraire le numéro d'issue
- Identifier si on est dans une boucle fix→verify (compter les itérations précédentes dans la conversation)

**Si on est à la 3ème itération de /fix consécutive** : stoppe et dis :
"C'est la 3ème tentative de correction. Les problèmes persistants sont :
<liste>
Je m'arrête pour éviter de tourner en rond. Tu veux que je détaille, qu'on ajuste le plan, ou qu'on reparte de zéro ?"

## Étape 4 — Délégation au sous-agent developer

Utilise `Task` avec :
- `subagent_type: "developer"`
- Prompt :

```
Les problèmes suivants ont été remontés par /verify. Corrige UNIQUEMENT ces points, sans toucher à autre chose.

<liste des problèmes avec fichier:ligne et nature>

Règles strictes :
- Corrige UNIQUEMENT les points listés ci-dessus
- Un commit par type de correction : `fix: address verify findings - <type>`
- Ne crée pas de nouveau fichier sauf si explicitement nécessaire (ex : test manquant demandé par /verify)
- Respecte toutes les conventions du projet
- Si une correction demande de modifier un fichier hors scope, STOP et signale-le

À la fin, fournis :
- Liste des commits créés
- Liste des fichiers modifiés
- Problèmes non corrigés (avec raison)
```

## Étape 5 — Re-verify automatique

Une fois le sous-agent terminé, **lance `/verify` automatiquement** en le signalant à l'utilisateur :

```
Correction terminée. Je relance /verify pour confirmer...
```

Puis exécute la logique de `/verify` (ou demande à l'utilisateur de lancer `/verify` si tu ne peux pas enchaîner directement).

## Étape 6 — Résultat

Selon le résultat du nouveau `/verify` :

**Si READY :**
```
✅ Tous les problèmes corrigés.
<N> commits de fix ajoutés.
Prochaine étape : /pr
```

**Si NOT READY et itération < 3 :**
```
⚠️ Il reste des problèmes. Tentative <X>/3.
Je peux relancer /fix automatiquement, ou tu peux intervenir.
Dis-moi : "continue" ou "stop".
```

**Si NOT READY et itération = 3 :**
```
❌ 3 tentatives épuisées. Arrêt automatique.
Problèmes restants :
<liste>
Il faut une intervention humaine ou une révision du plan.
```

## Ce que tu ne fais JAMAIS

- Coder toi-même
- Corriger des problèmes non remontés par /verify (même si tu en vois d'autres)
- Boucler indéfiniment (limite stricte à 3 itérations)
- Passer à `/pr` tout seul
