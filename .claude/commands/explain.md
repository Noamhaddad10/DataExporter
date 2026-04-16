---
description: Explique en français simple, sans jargon, ce que la dernière commande ou action a fait
argument-hint: "(optionnel) sujet précis à expliquer"
---

# /explain — Traduction en français simple

Ton but : faire comprendre à l'utilisateur ce qui vient de se passer, **sans jargon technique**, comme si tu expliquais à quelqu'un qui débute en programmation.

Réponds toujours en français, avec un ton bienveillant et pédagogique.

## Si un argument est fourni : $ARGUMENTS

L'utilisateur te demande d'expliquer un sujet précis. Concentre-toi là-dessus.

Exemples possibles :
- "le dernier commit"
- "pourquoi la PR est ROUGE"
- "ce que fait LoggerPduProcessor"
- "stop_event"
- "le partitioning Kafka"

## Si pas d'argument

Regarde dans la conversation ce qui vient de se passer (dernière commande, dernier rapport, dernière action technique) et explique-le.

## Règles de rédaction — STRICTES

### Interdits
- **Zéro jargon** sans le définir immédiatement
- Pas de code dans l'explication (sauf un mot-clé ou nom de fichier)
- Pas de listes à puces qui font peur — privilégie les paragraphes courts
- Pas de "simplement", "juste", "évidemment" (condescendant)
- Pas de "robuste", "performant", "élégant" (vide de sens)

### À faire
- Utilise des **analogies du quotidien** quand un concept technique apparaît
- **Cite les fichiers et fonctions** concrets, mais explique leur rôle en termes simples
- Structure en **paragraphes courts** (2-4 phrases max)
- Termine toujours par une **question de vérification** : "Est-ce que ça te parle ?" ou "Tu veux que je détaille un truc ?"

### Format type

```
## Ce qui vient de se passer

<1 paragraphe qui résume en 3-4 phrases, sans jargon>

## Le détail (si ça t'intéresse)

<2-3 paragraphes courts avec analogies, en citant les fichiers mais en expliquant leur rôle>

## Pourquoi c'est important

<1 paragraphe qui replace ça dans le projet global>

## À vérifier / comprendre de ton côté

<optionnel : 1-2 points que l'utilisateur pourrait regarder lui-même pour apprendre>

---
Tu veux que je creuse un point en particulier ?
```

## Exemples d'analogies recommandées

- **Kafka** = un bureau de poste qui trie le courrier en plusieurs boîtes (les partitions) pour que plusieurs facteurs (les consumers) distribuent en parallèle
- **stop_event** = un bouton "éteindre" que tous les processes regardent de temps en temps
- **PDU** = une enveloppe avec un code au début qui dit ce qu'il y a dedans
- **commit Git** = prendre une photo de ton code à un moment donné
- **PR** = une demande "est-ce que tu valides ma photo avant que je la mette dans l'album principal ?"
- **review** = un collègue qui regarde ta photo avant validation
- **sous-agent reviewer** = faire relire ton devoir par quelqu'un qui n'a pas vu tes brouillons, pour qu'il le juge avec des yeux neufs
- **multiprocessing** = avoir plusieurs personnes qui travaillent en même temps au lieu d'une seule

## Ce que tu ne fais JAMAIS

- Expliquer en restant technique (le but c'est l'accessibilité)
- Faire semblant de vulgariser tout en étant opaque
- Donner une leçon magistrale de 3 pages (max 15 lignes en général)
- Mentir ou inventer pour faire simple (mieux vaut dire "c'est technique, voici la version courte" que simplifier à tort)
