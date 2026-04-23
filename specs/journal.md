# Journal des PR

## 2026-04-16 — PR #1 : fix: audit report corrections (score 68→82+)

**Issue** : audit interne (pas d'issue GitHub)
**Classification** : ROUGE (GO avec réserves — réserve mineure acceptée)
**Commits** : parametrize SQL, mutable default fix, print→log, cleanup imports, exc_info, requirements.txt, .gitignore
**Résumé** : Correction de tous les problèmes IMPORTANT et CRITIQUE identifiés par l'audit du 2026-04-16. Injection SQL paramétrisée, mutable default corrigé, prints convertis en log, imports nettoyés, exc_info ajouté, engines disposés.

## 2026-04-16 — PR #3 : fix: audit 88/100 corrections (NameError, SQL_SERVER, exc_info)

**Issue** : #2
**Classification** : ROUGE (GO avec réserves — réserves mineures acceptées)
**Commits** : guard dead new_db block, centralize SQL_SERVER, add exc_info, untrack output/, audit reports + journal
**Résumé** : Correction du bug critique NameError (create_json/create_all_tables non définies), centralisation de localhost\SQLEXPRESS dans kafka_config.py, ajout exc_info=True manquant, untrack des .lzma, ajout des rapports d'audit.

## 2026-04-23 — PR #5 : fix: correctifs audit 2026-04-23 (shutdown hang, dead code, cleanup)

**Issue** : #4
**Classification** : ROUGE (GO avec réserves — 2 réserves non bloquantes acceptées : MAX_ACCUMULATE en constante de classe, close() utilise time.sleep au lieu de join)
**Commits** : add LoggerSQLExporter.close(), close main LSE on shutdown, dispose SQL engine on consumer, EntityLocations accumulation fix, tracked_tables [] fix, module docstrings, reorder close() before listener.stop()
**Résumé** : Correction des 2 bugs critiques et 2 importants de l'audit du 2026-04-23. Shutdown hang résolu via nouvelle méthode close() appelée sur le LSE principal avant listener.stop() et sur chaque consumer dans son finally. Branche dead code EntityLocations corrigée avec plafond MAX_ACCUMULATE=10. tracked_tables vide parse maintenant en []. Docstrings modules ajoutés. Item #5 de l'audit (retirer opendis.dis7) rétracté pendant le planning (import nécessaire, confirmé runtime).

## 2026-04-23 — PR #7 : fix: runtime bugs discovered by /smoke-test (log spam + partitioning)

**Issue** : #6
**Classification** : ORANGE (GO avec réserves — docstring module resynchronisée avant merge)
**Smoke test** : ✅ validé post-fix (935 msg/s, 0 perte, 4 consumers actifs)
**Commits** : swallow socket.timeout separately from OSError, partition by entity_id key not pdu_type, sync module docstring with new partitioning
**Résumé** : Correction des 2 bugs runtime découverts par le premier smoke test end-to-end du 2026-04-23. (1) `except OSError` catégorisait les timeouts socket (1 ERROR/s idle) → ajout d'`except socket.timeout: continue` avant. (2) `partition = pdu_type % N` routait tous les FirePdus sur la partition 2 (1/4 consumers actifs) → `key=entity_id` pour EntityState (cache coherence préservée), `key=None` sinon (round-robin Kafka). Débit end-to-end mesuré : 258 → 935 msg/s (x3.6). Zéro perte sur 5000 PDUs post-fix.
