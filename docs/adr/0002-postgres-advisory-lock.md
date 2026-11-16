# ADR 0002 — Verrou d'exécution par advisory lock PostgreSQL

- Statut : accepté
- Date : 2026-11-16

## Contexte

Deux exécutions du même job sur les mêmes données doivent être impossibles, quel que soit le lanceur : relance VTOM, lancement manuel, deux nœuds Quartz.

## Options étudiées

| Option | Pour | Contre |
|---|---|---|
| Convention d'ordonnancement (`concurrencyPolicy: Forbid`, une seule définition VTOM) | Aucun code | Ne couvre pas les autres lanceurs |
| Ligne « Running » dans une table + TTL | Portable | TTL à régler : trop court = double run, trop long = blocage après crash |
| Redis (Redlock) | Rapide | Une dépendance de plus, sémantique discutée |
| Advisory lock PostgreSQL (session) | Libéré par le serveur à la mort du processus, aucune dépendance de plus | Lié à PostgreSQL, incompatible avec PgBouncer en mode transaction |

## Décision

Advisory lock de session via `DistributedLock.Postgres`, clé `batch:<job>:<empreinte des paramètres>` par défaut.

Le verrou est pris sans attente : s'il est détenu, le run se termine en code 4.

La perte du verrou en cours de route (coupure de connexion) annule le job.

## Conséquences

- Pas de verrou orphelin : la détection des zombies devient triviale (verrou libre + statut Running = processus mort).
- L'abstraction `IJobLockProvider` permet une autre implémentation (SQL Server `sp_getapplock`, Redis) sans toucher au moteur.
