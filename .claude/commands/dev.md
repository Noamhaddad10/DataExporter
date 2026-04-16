---
description: Exécute le plan validé en déléguant au sous-agent developer
allowed-tools: Task, Bash(git:*), Read
---

# /dev — Exécution du plan

Tu délègues l'exécution du plan au sous-agent `developer`.

Réponds toujours en français.

## Étape 1 — Vérifications préalables

- `git branch --show-current` (doit être `feature/<N>-<slug>`)
- `git status` (doit être clean ou avec uniquement des changements liés à la tâche — si tu as un doute, demande à l'utilisateur avant de continuer)
- Extraire le numéro d'issue depuis le nom de la branche
- Vérifier que l'issue existe : `gh issue view <N> --json title`

## Étape 2 — Récupérer le plan

Le plan a été produit par `/plan` dans le tour de conversation précédent.
Si tu ne le vois pas dans l'historique (nouveau chat, reprise), dis :
"Je ne vois pas de plan validé dans la conversation. Relance `/plan` d'abord."

## Étape 3 — Délégation au sous-agent developer

Utilise l'outil `Task` avec :
- `subagent_type: "developer"`
- Prompt : le plan complet validé + ces instructions :

```
Tu as accès au plan ci-dessus qui a été validé par l'utilisateur.

Exécute-le étape par étape en respectant les conventions du projet Data Exporter.

Règles strictes :
- Une étape = un commit (ou plusieurs commits logiques si l'étape est grosse)
- Messages conventionnels : feat/fix/refactor/test/docs/chore
- Ne sors PAS du scope du plan
- Si tu rencontres un blocage, stoppe et signale-le sans improviser

À la fin, fournis un récap structuré :
- Commits créés (liste)
- Fichiers modifiés (liste)
- Tests ajoutés (liste)
- Blocages éventuels (ou "aucun")
```

## Étape 4 — Rapport final

Récupère la réponse du sous-agent et affiche :

```
✅ Développement terminé
Branche : feature/<N>-<slug>
Commits : <liste>
Fichiers modifiés : <liste>

Prochaine étape : /verify
```

Si le sous-agent a remonté un blocage, affiche-le clairement et NE PASSE PAS à l'étape suivante. Demande à l'utilisateur comment procéder.

## Ce que tu ne fais JAMAIS

- Coder toi-même (tu délègues au sous-agent)
- Merger, push, ouvrir une PR (ce n'est pas ton rôle)
- Passer à `/verify` tout seul (l'utilisateur ou `/ship` s'en charge)
