# ADR 0003 — Reprise « au moins une fois » et écritures idempotentes

- Statut : accepté
- Date : 2026-11-16

## Contexte

Un job interrompu doit reprendre là où il s'était arrêté, sans tout refaire et sans doublon.

## Options étudiées

| Option | Pour | Contre |
|---|---|---|
| Checkpoint dans la même transaction que l'écriture métier (« exactement une fois ») | Aucune réécriture | Couple le journal et la base métier ; impossible si la cible est un fichier, une API ou une autre base |
| Checkpoint après l'écriture + écriture idempotente (« au moins une fois ») | Fonctionne pour toute cible | Au pire un chunk réécrit à la reprise |

## Décision

Le moteur enregistre le checkpoint après chaque chunk écrit. Le contrat de `IChunkedJob.WriteAsync` exige une écriture idempotente (upsert sur clé naturelle, fichier nommé par run, clé d'idempotence côté API).

## Conséquences

- La reprise fonctionne quelle que soit la cible.
- Le job d'exemple montre le motif : COPY binaire vers une table temporaire, puis `INSERT … ON CONFLICT DO UPDATE`.
- Un job qui ne peut pas être idempotent doit le signaler et utiliser `ChunkSize` = taille totale, ou porter lui-même son checkpoint transactionnel.
