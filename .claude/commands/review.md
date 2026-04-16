---
description: Review stricte de la PR courante via un sous-agent reviewer cloisonné (sans accès au contexte du développement)
allowed-tools: Task
---

# /review — Audit de la PR

Tu délègues entièrement l'audit au sous-agent `reviewer`. C'est lui qui fait tout le travail.

**Règle cardinale : le sous-agent ne doit PAS recevoir le contexte du développement.** Son prompt doit contenir UNIQUEMENT l'instruction d'auditer la PR courante — rien d'autre.

Réponds toujours en français.

## Étape 1 — Délégation

Utilise `Task` avec :
- `subagent_type: "reviewer"`
- Prompt (garder volontairement minimal pour préserver l'isolement) :

```
Audite la PR courante du repo. Suis ton protocole strict.

Ta mission :
1. Récupérer la PR et l'issue liée via gh
2. Lire le diff
3. Appliquer les conventions du projet
4. Produire le rapport dans le format strict défini dans ton système
5. Classer la PR en VERT/ORANGE/ROUGE

Ne reçois aucun autre contexte de ma part. Lis l'issue et le diff, c'est tout.
```

**Ne passe AUCUNE autre information au sous-agent** : pas le plan, pas les commits, pas les rapports /verify, pas les rapports /fix. Son isolement est non négociable.

## Étape 2 — Récupération du rapport

Récupère le rapport complet produit par le sous-agent et affiche-le à l'utilisateur tel quel.

## Étape 3 — Extraction du verdict et de la classification

À la fin du rapport, ajoute une ligne de synthèse :

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Verdict : <GO | GO avec réserves | NO GO>
Classification : <VERT | ORANGE | ROUGE>
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Prochaine étape suggérée :
- Si GO + VERT/ORANGE : /merge (auto-merge possible)
- Si GO + ROUGE : /merge affichera un rapport et bloquera l'auto-merge
- Si GO avec réserves : décide avec l'utilisateur → /merge ou /fix
- Si NO GO : /fix
```

## Ce que tu ne fais JAMAIS

- Auditer toi-même (tu délègues TOUT au sous-agent)
- Donner au sous-agent des informations sur ce qui a été fait pendant le dev
- Négocier le verdict du sous-agent
- Lancer `/merge` tout seul
- Modifier le rapport du sous-agent avant affichage
