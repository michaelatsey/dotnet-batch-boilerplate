# Exploitation

## Lancer un job depuis un ordonnanceur externe

```bash
batch run <job> [-p Clé=Valeur]... [--restart]
```

- Une seule exécution, puis le processus s'arrête avec le code retour du job.
- Une ligne JSON sur **stdout** résume l'exécution. Tous les logs partent sur **stderr**.
- `--restart` est toujours sûr : il reprend un échec depuis son checkpoint, ne fait rien si le dernier run a réussi, et démarre à neuf s'il n'y a pas d'historique.

### VTOM (ou Control-M, Autosys)

Script de lancement type :

```bash
#!/usr/bin/env bash
set -u
export ConnectionStrings__Batch="$(cat /etc/batch/connection-string)"
export OTEL_EXPORTER_OTLP_ENDPOINT="http://otel-collector:4317"

/opt/batch/batch run import-invoices --restart -p InputFile="/data/in/invoices_${VTOM_DATE}.csv"
exit $?
```

Paramétrage conseillé des actions sur code retour :

| Code | Statut VTOM | Action |
|---|---|---|
| 0 | Terminé | Suite de la chaîne |
| 1 | Terminé avec alerte | Suite de la chaîne, ticket à l'équipe métier |
| 2 | En erreur | Arrêt de la chaîne, pas de relance automatique |
| 3 | En erreur | Relance automatique (le script passe déjà `--restart`), 2 tentatives espacées de 10 min |
| 4 | Terminé (information) | Aucune action : une autre instance fait le travail |
| 5, 6 | En erreur | Relance automatique avec `--restart` |
| 64, 78 | En erreur | Arrêt, correction de la définition ou de la configuration |

### Kubernetes

Voir [`deploy/k8s/cronjob.yaml`](../deploy/k8s/cronjob.yaml).

Points clés :

- `podFailurePolicy` arrête les relances sur les codes 2, 4, 64 et 78.
- `terminationGracePeriodSeconds` doit dépasser `BATCH_TERMINATION_GRACE_SECONDS` (60 par défaut).
- `concurrencyPolicy: Forbid` est une première barrière ; le verrou PostgreSQL reste la garantie.

## Arrêt propre

```
 SIGTERM ─▶ annulation du jeton ─▶ chunk en cours terminé ou abandonné
         ─▶ checkpoint du dernier chunk écrit ─▶ statut Cancelled ─▶ exit 5
         ─▶ au-delà de BATCH_TERMINATION_GRACE_SECONDS : processus tué
```

Un processus tué net (kill -9, OOM) est détecté au lancement suivant : son exécution passe en `Abandoned`, et `--restart` reprend depuis son checkpoint.

## Configuration

Toute option se surcharge par variable d'environnement (`:` devient `__`), avec ou sans préfixe `BATCH_`.

| Clé | Défaut | Rôle |
|---|---|---|
| `ConnectionStrings:Batch` | vide (mémoire) | Journal, verrou, tables des jobs |
| `Batch:HeartbeatInterval` | `00:00:30` | Fréquence de mise à jour des compteurs |
| `Batch:FinalizationTimeout` | `00:00:15` | Temps accordé à l'écriture du statut final |
| `Batch:Retry:MaxAttempts` | `3` | Nouvelles tentatives sur erreur transitoire (0 = aucune) |
| `Batch:Retry:BaseDelay` / `MaxDelay` | `1s` / `30s` | Backoff exponentiel avec jitter |
| `Batch:Jobs:<job>:ChunkSize` | selon le job | Taille d'une transaction |
| `Batch:Jobs:<job>:MaxDegreeOfParallelism` | selon le job | Parallélisme du traitement |
| `Batch:Jobs:<job>:SkipLimit` | selon le job | Rejets tolérés avant erreur fonctionnelle |
| `Batch:Jobs:<job>:Timeout` | selon le job | Durée maximale (`hh:mm:ss`) |
| `Batch:Jobs:<job>:Concurrency` | `SingleInstancePerParameters` | `SingleInstance`, `SingleInstancePerParameters`, `Unrestricted` |
| `Batch:Quartz:Store` | `Memory` | `Postgres` pour un cluster Quartz |
| `Batch:Quartz:ProvisionSchema` | `false` | Création des tables Quartz (développement) |
| `Batch:Schedules:<nom>` | — | `Job`, `Cron`, `TimeZone`, `Parameters` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | vide | Active l'export OpenTelemetry |
| `BATCH_TERMINATION_GRACE_SECONDS` | `60` | Délai avant arrêt forcé après SIGTERM |

La chaîne de connexion du verrou doit être une vraie session PostgreSQL : pas de PgBouncer en mode `transaction`.

## Supervision SQL

```sql
-- Dernier état de chaque job
SELECT * FROM batch.job_last_status ORDER BY job_name;

-- Exécutions en cours qui n'avancent plus (heartbeat > 5 min)
SELECT run_id, job_name, host_name, started_at, last_heartbeat_at, read_count, write_count
FROM batch.job_execution
WHERE status = 'Running' AND last_heartbeat_at < now() - interval '5 minutes';

-- Taux d'échec sur 7 jours
SELECT job_name,
       count(*) FILTER (WHERE status NOT IN ('Succeeded', 'CompletedWithWarnings')) * 100.0 / count(*) AS failure_pct
FROM batch.job_execution
WHERE started_at > now() - interval '7 days'
GROUP BY job_name;
```

Le job `purge-executions` (planifié par défaut à 03:15 Europe/Paris en mode `serve`) nettoie le journal au-delà de `RetentionDays`.

## Qualité

`TreatWarningsAsErrors` n'est actif qu'en CI (`CI=true`) : en local, un avertissement ne bloque pas l'exploration.

La CI compile en `Release`, exécute les tests unitaires puis les tests d'intégration (PostgreSQL via Testcontainers), construit l'image Docker et vérifie le contrat CLI.
