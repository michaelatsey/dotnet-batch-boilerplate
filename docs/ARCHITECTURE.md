# Architecture

> Un batch n'est pas un programme qui tourne la nuit.
> C'est un contrat avec un ordonnanceur, une base de données et une équipe d'exploitation.

Ce document explique les choix de ce socle.

Chaque section suit le même fil : l'anti-pattern, puis la solution, puis le pourquoi.

---

## 1. Le principe fondateur : le job ne connaît pas son déclencheur

### L'anti-pattern

Je ne partirais pas d'un `IJob` Quartz qui contient le code métier.

```
 ┌──────────────────────────────┐
 │ class ImportJob : IJob       │   ← Quartz dans la signature
 │   Execute(ctx)               │
 │     lit ctx.JobDataMap       │   ← paramètres non typés
 │     ouvre la base            │
 │     boucle sur 2M lignes     │   ← tout en mémoire
 │     catch { log }            │   ← code retour ? aucun
 └──────────────────────────────┘
```

Le jour où l'exploitation veut le lancer depuis VTOM, il faut tout réécrire.

Un ordonnanceur externe lance un **processus** et lit un **code retour**.

Quartz déclenche une **méthode** dans un processus déjà démarré.

Ce sont deux modèles d'exécution différents.

### La solution

Je séparerais trois couches, et seule la première connaît l'ordonnanceur.

```
 ┌─────────── Déclencheurs ───────────┐
 │  CLI (VTOM, cron, K8s) │  Quartz   │   adaptateurs fins
 └───────────┬────────────┴─────┬─────┘
             └────────┬─────────┘
                      ▼
 ┌─────────── IJobRunner ─────────────┐
 │  verrou · journal · reprise ·      │   moteur unique
 │  pipeline · retry · exit code      │
 └─────────────────┬──────────────────┘
                   ▼
 ┌─────────── Jobs ───────────────────┐
 │  IBatchJob<T> · IChunkedJob<...>   │   code métier pur
 └────────────────────────────────────┘
```

| Couche | Connaît | Ne connaît pas |
|---|---|---|
| Déclencheur | L'ordonnanceur, la ligne de commande | Le métier |
| Moteur | Le verrou, le journal, les chunks | L'ordonnanceur, le métier |
| Job | Ses données, ses règles | Qui l'a lancé, comment il est verrouillé |

Le moteur expose une seule méthode.

```csharp
public interface IJobRunner
{
    Task<RunOutcome> RunAsync(RunRequest request, CancellationToken cancellationToken);
}
```

La CLI l'appelle et rend `outcome.ExitCode` au système.

Quartz l'appelle et traduit `outcome.ExitCode` en résultat Quartz.

Les tests l'appellent directement.

> Un job qui sait qui l'a lancé est un job qu'on ne pourra plus lancer autrement.

---

## 2. Le contrat avec l'ordonnanceur

### L'anti-pattern

```
 $ import.exe
 [INFO] starting...
 [ERROR] something failed
 $ echo $?
 0                       ← VTOM croit que tout va bien
```

Ou l'inverse : tout échec renvoie `1`, et l'exploitation ne sait pas s'il faut relancer.

### La solution

Je donnerais à chaque situation un code stable, et à chaque code une action.

```
          ┌── 0  succès ───────────────────▶ chaîne suivante
          ├── 1  succès avec rejets ───────▶ alerte, chaîne suivante
          ├── 2  erreur fonctionnelle ─────▶ STOP, analyse métier
 run ─────┼── 3  erreur technique ─────────▶ relance --restart
          ├── 4  déjà en cours ────────────▶ rien (pas d'alerte rouge)
          ├── 5  annulé (SIGTERM) ─────────▶ relance --restart
          ├── 6  timeout ──────────────────▶ relance --restart, dimensionner
          ├── 64 usage invalide ───────────▶ corriger la définition du job
          └── 78 configuration invalide ───▶ corriger l'hôte
```

Les canaux sont séparés.

```
 stdout ──▶ {"job":"import-invoices","runId":"…","status":"CompletedWithWarnings","exitCode":1,"read":2500,"written":2496,"skipped":4}
 stderr ──▶ 2026-11-16T02:30:01.123Z info: Batch.Core.Execution.JobRunner[1005] Starting 'import-invoices' run …
```

| Canal | Contenu | Lecteur |
|---|---|---|
| Code retour | Une décision | L'ordonnanceur |
| stdout | Une ligne JSON | Un script, un parseur VTOM |
| stderr | Les logs | L'humain, le collecteur de logs |
| Journal SQL | L'historique complet | La supervision, l'audit |

Les codes 64 et 78 suivent la convention `sysexits.h`.

Ils sont testés comme un contrat (`Exit_codes_are_a_stable_contract`) : les changer casse l'ordonnancement.

> Le code retour est l'API publique d'un batch. Elle se versionne comme une API.

---

## 3. La performance : un pipeline par chunks

### L'anti-pattern

```
 var all = await db.Invoices.ToListAsync();     ← 2 M lignes en mémoire
 foreach (var x in all)
 {
     Process(x);
     db.Add(x);
     await db.SaveChangesAsync();               ← 2 M allers-retours
 }
```

Mémoire proportionnelle au volume, un aller-retour par ligne, aucun chevauchement entre lecture et écriture.

### La solution

Je découperais l'exécution en trois étages reliés par des canaux bornés.

```
 ┌─────────┐  Channel(2)  ┌──────────────┐  Channel(2)  ┌──────────┐
 │ Lecture │ ───chunks──▶ │ Traitement   │ ───chunks──▶ │ Écriture │──▶ checkpoint
 │ (flux)  │              │ ∥ DOP borné  │              │ (ordre)  │
 └─────────┘              └──────────────┘              └──────────┘
   chunk N+2                 chunk N+1                    chunk N
```

Les trois étages travaillent en même temps.

Quand l'écriture ralentit, le canal se remplit et la lecture attend.

C'est la contre-pression : la mémoire reste constante, quel que soit le volume.

| Étage | Concurrence | Pourquoi |
|---|---|---|
| Lecture | Séquentielle, en flux | Pagination keyset, `IAsyncEnumerable` |
| Traitement | Parallèle, `MaxDegreeOfParallelism` | Validation, transformation, appels externes |
| Écriture | Séquentielle, une transaction par chunk | Ordre garanti, checkpoint exact |

Le parallélisme ne casse pas l'ordre.

Chaque résultat garde l'index de son item dans le chunk.

```csharp
var results = new TOut?[chunk.Items.Count];
await Parallel.ForEachAsync(Enumerable.Range(0, chunk.Items.Count), parallel,
    async (i, ct) => results[i] = await ProcessItemAsync(chunk.Items[i], ct));
```

L'écriture du job d'exemple passe par un COPY binaire dans une table temporaire, puis un upsert.

```
 chunk (1 000 lignes) ──COPY BINARY──▶ invoice_stage (temp) ──INSERT … ON CONFLICT──▶ billing.invoice
                       1 aller-retour                         1 requête
```

Côté démarrage, l'exécutable est publié en ReadyToRun : un ordonnanceur qui lance 300 jobs courts par nuit paie le JIT 300 fois sinon.

> On ne rend pas un batch rapide en optimisant la boucle. On le rend rapide en supprimant les allers-retours et en faisant se chevaucher les étapes.

---

## 4. La concurrence, à deux niveaux

### L'anti-pattern

```
 VTOM relance par erreur ──▶ import.exe ─┐
                                          ├──▶ mêmes factures écrites deux fois
 un collègue lance à la main ─▶ import.exe ┘

 « On a mis concurrencyPolicy: Forbid dans le CronJob. »
   └─▶ ne protège pas d'un lancement manuel, ni d'un autre ordonnanceur
```

Une convention d'ordonnancement n'est pas une garantie.

### La solution

Je mettrais la garantie dans la base, là où tous les lanceurs se rencontrent.

```
 nœud A : run import -p Date=2026-11-16 ──┐
                                           ├─▶ pg_try_advisory_lock(hash("batch:import:<empreinte>"))
 nœud B : run import -p Date=2026-11-16 ──┘        A ✔  B ✘ → exit 4

 nœud C : run import -p Date=2026-11-17 ─────▶ autre empreinte → s'exécute en parallèle
```

| Mode | Clé du verrou | Usage |
|---|---|---|
| `SingleInstancePerParameters` (défaut) | `batch:<job>:<empreinte des paramètres>` | Deux dates en parallèle, jamais deux fois la même |
| `SingleInstance` | `batch:<job>` | Purge, maintenance, export global |
| `Unrestricted` | aucune | Jobs naturellement idempotents |

L'empreinte est un SHA-256 des paramètres normalisés (clés en minuscules, triées).

`-p Date=2026-11-16 -p Mode=full` et `-p mode=full -p date=2026-11-16` donnent la même clé.

L'advisory lock PostgreSQL est porté par une session.

Si le processus meurt, le serveur le libère : pas de TTL à régler, pas de verrou orphelin.

Si la connexion qui le porte tombe en cours de route, `LostToken` annule le job : on préfère un arrêt à un double traitement.

Quartz ajoute `[DisallowConcurrentExecution]` : c'est une protection locale au nœud, le verrou reste la garantie globale.

> La concurrence se garantit là où tous les lanceurs se croisent. Chez nous, c'est la base.

---

## 5. La reprise et l'idempotence

### L'anti-pattern

```
 run 1 : ████████████████████░░░░░░░░  crash à 70 %
 run 2 : ████████████████████████████  tout recommencer, 2 h de plus
                                        et doublons si l'écriture n'est pas idempotente
```

### La solution

Je commiterais un checkpoint après chaque chunk écrit, et je rendrais l'écriture rejouable.

```
 chunk 1 ✔ cp=1000   chunk 2 ✔ cp=2000   chunk 3 ✘ crash
                                 │
 run --restart ──────────────────┘ ReadAsync(resumeAfter: "2000") → chunk 3, 4, …
```

Le moteur n'offre pas « exactement une fois ».

Il offre « au moins une fois » plus une écriture idempotente, ce qui revient au même pour le métier et reste honnête techniquement.

```
 écriture du chunk N ✔ ─── crash ici ───▶ checkpoint N non enregistré
 --restart  ─▶ chunk N réécrit ─▶ upsert sur la clé naturelle ─▶ aucun doublon
```

| Situation | Comportement de `--restart` |
|---|---|
| Dernière exécution en échec, annulée, en timeout, abandonnée | Reprise depuis son checkpoint |
| Dernière exécution réussie | Rien à faire, code 0 |
| Aucune exécution | Démarrage à neuf |

L'ordonnanceur peut donc toujours relancer avec `--restart` : c'est sans risque.

Les zombies sont détectés sans heartbeat.

```
 verrou obtenu  +  exécution « Running » pour la même clé
        └──────────────▶ son processus est forcément mort ──▶ Abandoned
```

Le heartbeat reste utile, mais pour la supervision : « ce job avance-t-il ? ».

> Un batch qu'on ne peut pas relancer sans réfléchir n'est pas un batch de production.

---

## 6. La résilience : savoir quoi retenter

### L'anti-pattern

```
 retry(5) sur toute exception
   ├── timeout réseau      → retenter : ✔ utile
   ├── violation de clé    → retenter : ✘ 5 échecs identiques, 5 fois plus de logs
   └── fichier absent      → retenter : ✘ le fichier n'apparaîtra pas
```

### La solution

Je classerais les erreurs avant de décider.

```
 Exception
   ├── BatchFunctionalException ──────▶ jamais retentée ─▶ exit 2
   ├── SkippableItemException ────────▶ item rejeté, compté ─▶ exit 1 ou 2 (SkipLimit)
   ├── transitoire (DbException.IsTransient, Timeout, IO, HTTP)
   │        └──▶ Polly : retry exponentiel + jitter ─▶ si épuisé, exit 3
   └── le reste ──────────────────────▶ exit 3
```

| Erreur | Exemple | Retry | Code |
|---|---|---|---|
| Fonctionnelle | Fichier absent, règle métier | Non | 2 |
| Item invalide | Date illisible sur une ligne | Non, item rejeté | 1 (ou 2 au-delà du seuil) |
| Transitoire | Coupure réseau, deadlock | Oui | 3 si épuisé |
| Technique | Bug, contrainte violée | Non | 3 |

Le retry s'applique au traitement d'un item et à l'écriture d'un chunk, pas au job entier.

Le classement est remplaçable en DI (`ITransientErrorClassifier`) pour reconnaître les erreurs d'un SDK tiers.

> Retenter une erreur déterministe ne la rend pas transitoire. Ça la rend plus chère.

---

## 7. L'observabilité

### L'anti-pattern

```
 [INFO] Processing...
 [INFO] Processing...
 [ERROR] Failed
```

Quel run ? Quels paramètres ? Combien de lignes traitées avant l'erreur ?

### La solution

Je donnerais trois vues complémentaires, toutes reliées par le `run_id`.

```
                    run_id = 01936f2e-…
          ┌───────────────┼────────────────┐
          ▼               ▼                ▼
   Journal SQL       Traces OTel        Logs structurés
   batch.job_execution  batch.run          scope batch.run_id
   (statut, compteurs,  └ batch.chunk.write  EventId stables
    checkpoint, durée)     (taille, n°)     (1000-1010, 2000…)
```

| Vue | Question à laquelle elle répond |
|---|---|
| Journal SQL | Qu'est-ce qui a tourné, avec quel résultat, où reprendre ? |
| Métriques | Combien d'items par seconde ? Quelle durée de chunk ? Taux d'échec ? |
| Traces | Quel chunk a été lent, et pourquoi ? |
| Logs | Que s'est-il passé exactement ? |

La vue `batch.job_last_status` donne le dernier état de chaque job en une requête, prête pour Grafana.

L'export OTLP s'active avec la variable standard `OTEL_EXPORTER_OTLP_ENDPOINT`, sans code.

Les logs passent par des `[LoggerMessage]` générés : zéro allocation quand le niveau est désactivé.

> Si l'exploitation doit lire le code pour comprendre un incident, l'observabilité a échoué.

---

## 8. L'évolutivité

### L'anti-pattern

```
 Jobs/
 ├── Readers/        ImportReader.cs   ExportReader.cs   PurgeReader.cs
 ├── Processors/     ImportProcessor.cs ExportProcessor.cs
 ├── Writers/        ImportWriter.cs   ExportWriter.cs
 └── Models/         …
```

Ajouter un job, c'est toucher quatre dossiers.

### La solution

Je découperais par job, pas par couche technique.

```
 Jobs/
 ├── ImportInvoices/
 │   ├── ImportInvoicesJob.cs       ← paramètres, lecture, traitement, écriture
 │   └── invoice_schema.sql
 └── PurgeExecutions/
     └── PurgeExecutionsJob.cs
```

Les réglages se surchargent par l'exploitation, sans recompilation.

```
 code (défauts)              appsettings.json             variable d'environnement
 ChunkSize = 1000     ──▶    Batch:Jobs:import-invoices   ──▶  Batch__Jobs__import-invoices__ChunkSize=5000
                             :ChunkSize = 2000                  (le dernier gagne)
```

### Aujourd'hui / Demain

```
 AUJOURD'HUI                                  DEMAIN
 ───────────                                  ──────
 un exécutable `batch`                        `batch-billing`, `batch-hr` : un hôte par domaine
   └ tous les jobs                              └ même Core, même contrat, déploiements séparés

 VTOM ─▶ batch run                            VTOM ─▶ batch run          (inchangé)
 Quartz ─▶ batch serve                        Kafka/RabbitMQ ─▶ adaptateur consommateur ─▶ IJobRunner

 un chunk = une transaction locale            partitionnement : N pods, chacun une plage de clés
                                              (un paramètre Partition, une clé de verrou par partition)

 journal PostgreSQL                           IJobExecutionStore SQL Server / autre
 (EfJobExecutionStore)                        (une implémentation, rien d'autre ne change)
```

| Évolution | Ce qui change | Ce qui ne change pas |
|---|---|---|
| Nouvel ordonnanceur | Un adaptateur de déclenchement | Moteur, jobs, codes retour |
| Nouvelle base pour le journal | Une implémentation de `IJobExecutionStore` et `IJobLockProvider` | Moteur, jobs |
| Découpage par domaine | Un `Batch.Host.*` par domaine | Core, Abstractions |
| Montée en charge d'un job | Options (`ChunkSize`, DOP) puis partitionnement | Le contrat `IChunkedJob` |

> Une architecture évolutive n'anticipe pas toutes les évolutions. Elle rend chacune locale.

---

## 9. Ce que je ne ferais pas

| Tentation | Pourquoi je m'abstiendrais |
|---|---|
| Spring Batch en .NET (steps, jobs, flows en XML) | La complexité dépasse le besoin de 95 % des batchs |
| Hangfire comme moteur | Excellent pour des tâches de fond web, pas pour un contrat code retour avec VTOM |
| Migrations EF générées pour le journal | Un script SQL relu par le DBA est plus sûr en production |
| `OFFSET` pour paginer la lecture | Coût quadratique et lignes sautées si la table bouge ; le keyset est linéaire et stable |
| MediatR, CQRS dans un batch | Un job est déjà une commande ; l'indirection n'apporte rien ici |
| Native AOT dès le départ | EF Core et la liaison de configuration par réflexion le compliquent ; ReadyToRun suffit |

---

## 10. Récapitulatif

| Préoccupation | Mécanisme | Où |
|---|---|---|
| Indépendance vis-à-vis de l'ordonnanceur | `IJobRunner` unique, adaptateurs fins | `Batch.Core/Execution`, `Batch.Hosting.Quartz`, `Batch.Host/Cli` |
| Contrat d'exploitation | Codes retour stables, JSON sur stdout, logs sur stderr | `ExitCodes`, `CliCommands` |
| Performance | Pipeline 3 étages, `Channel` bornés, COPY binaire, ReadyToRun | `ChunkEngine`, `ImportInvoicesJob` |
| Concurrence intra-job | `Parallel.ForEachAsync` borné, ordre préservé | `ChunkEngine` |
| Concurrence inter-instances | Advisory lock PostgreSQL par empreinte de paramètres | `PostgresJobLockProvider` |
| Reprise | Checkpoint par chunk, `--restart`, upsert idempotent | `JobRunner`, `EfJobExecutionStore` |
| Robustesse | Classement des erreurs, Polly, zombies, perte de verrou, SIGTERM | `BatchResilience`, `JobRunner` |
| Observabilité | Journal SQL, OpenTelemetry, `[LoggerMessage]` | `BatchTelemetry`, `001_batch_journal.sql` |
| Évolutivité | Un dossier par job, options surchargeables, ports remplaçables | `Batch.Jobs`, `BatchJobOptions` |

### La règle d'or

> **Le métier dit quoi faire. Le moteur dit comment le faire sûrement. L'ordonnanceur dit seulement quand.**
