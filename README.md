# .NET 10 Batch Boilerplate

Un socle de batch .NET 10 **indépendant de l'ordonnanceur**.

Le même job tourne lancé par VTOM, Control-M, Autosys, cron ou un CronJob Kubernetes (`batch run ...`), ou planifié par Quartz.NET 4 embarqué (`batch serve`).

Le code métier ne change pas d'une ligne.

```
 VTOM / Control-M / cron / K8s            Quartz.NET 4 (serve)
            │  batch run <job> -p k=v            │  trigger cron
            ▼                                    ▼
      ┌───────────── IJobRunner : un seul point d'entrée ─────────────┐
      │ paramètres typés ─▶ verrou distribué ─▶ journal ─▶ reprise     │
      │ ─▶ pipeline par chunks (Channels) ─▶ retry ─▶ OTel ─▶ exit code │
      └───────────────────────────────────────────────────────────────┘
                                 ▼
                 Jobs/ImportInvoices   Jobs/PurgeExecutions   ...
```

---

## Ce que le socle fournit

| Préoccupation | Réponse |
|---|---|
| Contrat avec l'ordonnanceur | Codes retour stables (0 à 6, 64, 78), une ligne JSON sur stdout, les logs sur stderr |
| Concurrence entre instances | Advisory lock PostgreSQL par `job + empreinte des paramètres`, perte du verrou détectée |
| Concurrence dans un job | Pipeline lecture ▸ traitement ▸ écriture sur `Channel` bornés, parallélisme borné, ordre préservé |
| Volume | Lecture en flux (`IAsyncEnumerable`), mémoire constante, écriture bulk (COPY binaire) |
| Reprise | Checkpoint par chunk, `--restart` repart du dernier chunk commité, écriture idempotente |
| Données invalides | Rejets comptés et bornés (`SkipLimit`), code 1 en dessous du seuil, 2 au-dessus |
| Pannes transitoires | Polly v8 : retry exponentiel avec jitter, uniquement sur les erreurs transitoires |
| Processus tué (kill -9, OOM) | Exécution « zombie » marquée `Abandoned` au lancement suivant |
| Arrêt propre | SIGTERM ▸ annulation ▸ fin du chunk ▸ checkpoint ▸ statut ▸ code 5 |
| Observabilité | Journal SQL, traces et métriques OpenTelemetry, logs structurés avec `run_id` |
| Évolution | Un dossier par job, configuration surchargeable par l'exploitation sans redéploiement |
| Chaîne d'approvisionnement | Lockfiles NuGet versionnés, restore verrouillé en CI et dans l'image, audit NuGet des vulnérabilités |

## Démarrage rapide

```bash
docker compose up -d                                   # PostgreSQL + tableau de bord OTel
export DOTNET_ENVIRONMENT=Development
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317

dotnet run --project src/Batch.Host -- db init          # schéma du journal + tables métier
dotnet run --project src/Batch.Host -- list             # jobs et paramètres
dotnet run --project src/Batch.Host -- run import-invoices -p InputFile=samples/invoices.csv
echo $?                                                 # 1 : 4 lignes invalides rejetées sur 2 500
dotnet run --project src/Batch.Host -- history
dotnet run --project src/Batch.Host -- serve            # Quartz, planifications de Batch:Schedules
```

Traces et métriques : http://localhost:18888.

Sans chaîne de connexion, le moteur tourne avec un journal et un verrou en mémoire (`purge-executions` fonctionne, `import-invoices` exige PostgreSQL).

## Codes retour

| Code | Signification | Action conseillée côté ordonnanceur |
|---|---|---|
| 0 | Succès | Suite de la chaîne |
| 1 | Succès avec rejets sous le seuil | Alerte, suite possible |
| 2 | Erreur fonctionnelle | Arrêt, analyse métier, pas de relance |
| 3 | Erreur technique après retries | Relance automatique avec `--restart` |
| 4 | Déjà en cours | Ni relance, ni alerte rouge |
| 5 | Annulé (SIGTERM) | Relance avec `--restart` |
| 6 | Timeout | Relance avec `--restart`, revoir le dimensionnement |
| 64 | Usage invalide (job, paramètre) | Corriger la définition du job |
| 78 | Configuration invalide | Corriger la configuration de l'hôte |

Détails d'exploitation (VTOM, Kubernetes, supervision SQL) : [docs/OPERATIONS.md](docs/OPERATIONS.md).

## Écrire un job

```
src/Batch.Jobs/
└── ExportAccounting/
    ├── ExportAccountingJob.cs      ← paramètres + lecture + traitement + écriture
    └── export_schema.sql           ← tables propres au job (optionnel)
```

```csharp
public sealed class ExportAccountingParameters
{
    [Required] public DateOnly Date { get; set; }
}

public sealed class ExportAccountingJob(NpgsqlDataSource db)
    : IChunkedJob<ExportAccountingParameters, LedgerRow, ExportLine>
{
    public IAsyncEnumerable<LedgerRow> ReadAsync(JobContext<ExportAccountingParameters> ctx, string? resumeAfter, CancellationToken ct) => ...; // keyset, jamais OFFSET
    public ValueTask<ExportLine?> ProcessAsync(LedgerRow row, JobContext<ExportAccountingParameters> ctx, CancellationToken ct) => ...;
    public Task WriteAsync(IReadOnlyList<ExportLine> chunk, JobContext<ExportAccountingParameters> ctx, CancellationToken ct) => ...; // idempotent
    public string CheckpointOf(LedgerRow row) => row.Id.ToString();
}
```

Puis une ligne dans `JobsRegistration.cs` :

```csharp
builder.AddChunkedJob<ExportAccountingJob, ExportAccountingParameters, LedgerRow, ExportLine>(
    "export-accounting", o => { o.ChunkSize = 2_000; o.MaxDegreeOfParallelism = 4; o.Timeout = TimeSpan.FromHours(1); });
```

L'exploitation peut ensuite ajuster `Batch:Jobs:export-accounting:ChunkSize` sans recompiler.

## Structure

| Projet | Rôle | Dépend de |
|---|---|---|
| `Batch.Abstractions` | Contrats : `IBatchJob`, `IChunkedJob`, `JobContext`, `ExitCodes` | rien (Logging.Abstractions) |
| `Batch.Core` | Runner, pipeline par chunks, verrou, journal, retry, télémétrie | Abstractions |
| `Batch.Infrastructure.PostgreSql` | Journal EF Core 10, advisory lock, scripts de schéma | Core |
| `Batch.Hosting.Quartz` | Adaptateur Quartz.NET 4, planifications déclaratives | Core |
| `Batch.Jobs` | Les jobs métier, un dossier par job | Core, Infrastructure |
| `Batch.Host` | L'exécutable `batch` : `run`, `list`, `history`, `serve`, `db init` | tout |

Choix et compromis : [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) et [docs/adr](docs/adr).

## Stack

.NET 10 · C# 14 · System.CommandLine 2.0 · Quartz.NET 4.7 · EF Core 10 + Npgsql 10 · DistributedLock.Postgres · Polly 8 · OpenTelemetry · xUnit v3 · Testcontainers · Central Package Management · `.slnx`

Versions relevées sur nuget.org le 16/11/2026 et centralisées dans `Directory.Packages.props`. Dependabot propose les montées de version.

Les dépendances transitives sont figées par des lockfiles NuGet (`packages.lock.json`, versionnés). La CI et le Dockerfile restaurent en mode verrouillé : un paquet qui dérive fait échouer le build au lieu d'entrer en silence dans l'image. Après une modification de `Directory.Packages.props`, lancer `dotnet restore Batch.slnx` et commiter les lockfiles mis à jour. Les projets livrés (`src/`) déclarent les runtimes de l'image (`linux-x64`, `linux-arm64`) dans `src/Directory.Build.props`, pour que le restore spécifique au runtime du Dockerfile reste verrouillable.

Le SDK est figé (`global.json`, `rollForward: disable`) parce que les lockfiles en dépendent : il ajoute implicitement des paquets à sa propre version (`Microsoft.NET.ILLink.Tasks`, analyseurs AOT). Monter le SDK se fait en une seule PR :

```bash
# 1. global.json : "version": "<nouvelle version>"
# 2. src/Batch.Host/Dockerfile : FROM mcr.microsoft.com/dotnet/sdk:<nouvelle version>
dotnet restore Batch.slnx --force-evaluate   # 3. régénère les lockfiles avec le nouveau SDK
git add global.json src/Batch.Host/Dockerfile '**/packages.lock.json'
```

## État

La CI GitHub Actions compile la solution en `Release` avec les avertissements traités en erreurs (0 avertissement). Elle exécute 37 tests unitaires et 8 tests d'intégration sur un PostgreSQL 17 réel (Testcontainers), puis construit l'image Docker et vérifie le contrat CLI (`list`, code 64 sur un job inconnu).
