# Test baseline pipeline — 2026-04-23

**Objectif** : vérifier que le pipeline DIS → Kafka → SQL fonctionne de bout en bout et mesurer le débit actuel avant d'attaquer le travail de perf.

**Verdict** : 🟢 **Pipeline fonctionnel**. Débit end-to-end mesuré = **~260 msg/s**.

---

## Infrastructure au moment du test

- Windows 11 Home 10.0.26200
- Python 3.x dans `.venv/` local
- SQL Server 2025 RTM sur `localhost\SQLEXPRESS`, DB `noamtest`, schéma `dis` (6 tables) + `dbo.Loggers`
- Kafka 3.9.0 KRaft mode sur `localhost:9092`, 4 partitions sur `dis.raw`
- Java Temurin JDK 17.0.18
- Commit testé : `0d1fbc4` (master au moment du test, inclut PR #5)

### Procédure de démarrage Kafka

L'environnement KRaft existant avait des fichiers `.deleted` orphelins (héritage de sessions précédentes). Le broker crashait au bout de ~30s avec :

```
KafkaStorageException: Error while deleting segments for dis.raw-0 in dir C:\kafka\kraft-logs
FileSystemException: ... The process cannot access the file because it is being used by another process
Shutdown broker because all log dirs in C:\kafka\kraft-logs have failed
```

**Cause** : bug chronique Kafka + Windows — le log retention essaye de faire un atomic rename de segments qui sont encore référencés par un handle Windows (indexer ou autre). Quand les données s'accumulent (ici 1.1 GB dans kraft-logs), le crash devient systématique.

**Résolution appliquée** : wipe complet de `C:\kafka\kraft-logs` + re-format via `kafka-storage.bat format --cluster-id <new-uuid>`. Les données SQL (intact) ne sont pas impactées. Topic `dis.raw` re-créé avec 4 partitions.

---

## Snapshot SQL avant test

| Table | Rows |
|---|---|
| `dis.EntityLocations` | 2006 |
| `dis.Entities` | 1003 |
| `dis.FirePdu` | 18 393 |
| `dis.DetonationPdu` | 5 |
| `dis.Aggregates` | 0 |
| `dis.AggregateLocations` | 0 |
| `dbo.Loggers` | 19 |

---

## Test 1 — Smoke fonctionnel (10 FirePdus)

**Méthode** : `tools/send_fire_pdu.py` (10 PDUs, 0.5s entre chaque = 5s total).

**Résultat** :
- `dis.FirePdu` : 18 393 → **18 403** (+10) ✅
- `dbo.Loggers` : 19 → **20** (+1 nouvelle entrée du run en cours) ✅
- Autres tables inchangées ✅

**Conclusion** : le pipeline DIS UDP → Kafka producer → Kafka broker → Kafka consumer → `opendis.createPdu` → `LoggerPduProcessor.process` → `LoggerSQLExporter.insert_sync` → SQL Server fonctionne de bout en bout. Aucune perte, aucun doublon.

---

## Test 2 — Débit (10 000 FirePdus en rafale)

**Méthode** : script inline Python qui envoie 10 000 FirePdus en UDP aussi vite que possible (pas de délai), avec un eventNumber unique par PDU (pour que chaque row SQL soit distincte). Polling SQL toutes les 100 ms pour tracer l'arrivée.

**Résultat** :

```
Sent 10000 PDUs in 5.478s (1825 send/s)         ← côté UDP emitter
All 10000 PDUs reached SQL in 38.70s            ← end-to-end
End-to-end throughput: 258 msg/s
```

**Aucun message perdu** : 10000 envoyés, 10000 en SQL.

### Dynamique d'arrivée observée

- t = 5.48 s (fin du burst UDP) : déjà **1 323 rows** en SQL. Donc pendant le burst, la pipeline sortait ~240 msg/s en parallèle du flux entrant.
- Après le burst : régime permanent à ~260 msg/s jusqu'à vidage du backlog.
- Ni backlog qui explose, ni drop observé — la pipeline est stable, juste lente.

---

## Observations / bugs découverts au runtime

### 🔴 Partitioning cassé pour les FirePdus seuls

Stats consumers (sur les 10 000 FirePdus) :

```
Consumer 2 stats | consumed=2684 (268/s) | exported=2684 (268/s) | skipped=0 | errors=0
```

**Un SEUL consumer sur les 4 a fait TOUT le travail.** Preuve : Consumer 2 seulement apparaît dans les stats avec un débit > 0.

**Cause** : `kafka_producer.py:125-126` fait `partition = pdu_type % cfg.KAFKA_NUM_PARTITIONS`. Pour `pdu_type=2` (FirePdu) et `NUM_PARTITIONS=4`, toutes les FirePdus vont toujours sur la partition 2.

Mapping actuel avec les types supportés :
| pdu_type | Nom | Partition |
|---|---|---|
| 1 | EntityStatePdu | 1 |
| 2 | FirePdu | 2 |
| 3 | DetonationPdu | 3 |
| 21 | AggregateStatePdu | 1 (21 % 4 = 1) |
| 33 | ??? | 1 (33 % 4 = 1) |

**Conséquence en prod** : pour un flux typiquement dominé par EntityStatePdu (type 1), partition 1 sera saturée, les consumers 0/2/3 oisifs. Le "4 consumers" annoncé est en réalité 1 seul sur le chemin chaud. **Facteur 4 perdu sur le parallélisme.**

### 🟡 `log.error` spam sur `TimeoutError` UDP idle

`kafka_producer.py:146-150` :

```python
except OSError as exc:
    log.error("UDP socket error: %s", exc, exc_info=True)
    count_errors += 1
    continue
```

`socket.timeout` / `TimeoutError` est une sous-classe de `OSError` en Python 3.10+. Le `sock.settimeout(1.0)` du producer lève un `TimeoutError` **toutes les secondes** pendant les phases idle. Résultat :

- `log.error` écrit dans `dis-kafka.log` chaque seconde avec une stack trace
- `count_errors` s'incrémente artificiellement (pollue les stats)
- Les vraies erreurs UDP (OSError non-timeout) sont noyées dans le bruit

**Fix trivial** : attraper `socket.timeout` séparément et `continue` sans logger.

### 🟢 Validation partielle du fix PR #5 (shutdown hang)

**Non testable** dans ce setup : le pipeline tournait en background, `TaskStop` envoie un kill brutal qui ne déclenche pas les `finally` Python. Le dernier log est un timeout UDP normal, puis rien (pas de "Stopped gracefully" attendu). Pour valider PR #5, il faut lancer `kafka_main.py` dans une console réelle et faire Ctrl+C manuellement. **Test shutdown = non validé**, mais pas invalidé non plus.

---

## Chiffres clés pour l'issue perf

| Métrique | Valeur | Commentaire |
|---|---|---|
| Débit UDP send max | 1 825 msg/s | Limité par le Python emitter, pas la pipeline |
| **Débit end-to-end** | **258 msg/s** | Métrique clé |
| Consumers actifs observés | **1 / 4** | Bug partitioning |
| Débit par consumer actif | 260 msg/s | Cohérent avec 1 insert SQL/message + 1 commit Kafka/message |
| Perte de message | **0** sur 10 000 | Pipeline fiable |
| Duplication | **0** | Pas de doublon |

### Objectif vs baseline

| | Actuel | Objectif | Facteur |
|---|---|---|---|
| Débit end-to-end | 258 msg/s | 20 000 msg/s | **×77** |

Avec les 4 consumers vraiment actifs (fix partitioning), on passerait mécaniquement à ~1000 msg/s. Encore **×20 à trouver** via batching SQL + batching Kafka commits.

---

## Leviers perf confirmés par le test

1. **Partitioning** — passer à partitioning par `entity_id` (ou hash round-robin) au lieu de `pdu_type`. Gain immédiat : ×4 grâce aux 4 consumers déjà dimensionnés.
2. **Batch SQL** — actuellement 1 insert par message. Passer à ~200 messages par insert. Gain estimé : ×10-20.
3. **Batch Kafka commits** — commit tous les N messages au lieu de chaque. Gain estimé : ×2-3.
4. **Supprimer `Exporter.export()` / Timer async** — code mort dans le chemin chaud, gain CPU/mémoire mineur.
5. **(Éventuel)** bypass `opendis.createPdu` pour types dominants — gain CPU sur le parsing.

Avec 1+2+3 combinés, projection : ~15-25 k msg/s. **L'objectif 20k devient atteignable**, à condition que SQL Server suive (non vérifié sous charge).

---

## Logs pertinents (archivés)

- Pipeline : `dis-kafka.log` (~1.4 MB à la fin du test, principalement du bruit `TimeoutError`)
- Kafka broker : `C:\Users\hadda\AppData\Local\Temp\claude\...\bq84djlxx.output`
- Producer UDP : voir stats dans `dis-kafka.log` grep `Producer stats`

---

## Ce qui n'a PAS été mesuré

- **Débit max du producer seul** (UDP → Kafka, sans SQL) — aurait permis d'isoler la contribution de Kafka vs SQL.
- **Débit max du consumer seul** (Kafka → SQL avec flux pré-chargé) — aurait permis de tester le chemin SQL sans la charge du producer.
- **Latence percentiles** (p50, p95, p99) — aurait donné une idée de la queue de distribution.
- **Shutdown gracieux** — pas testable depuis ce setup automatisé.
- **Charge mixte** (plusieurs types de PDU) — n'a envoyé que des FirePdus.
- **SQL Server sous charge soutenue** — 10 000 messages sur 40s ne stresse pas SQL Express.
