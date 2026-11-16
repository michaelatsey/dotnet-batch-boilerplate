-- Journal d'exécution des batchs. Idempotent : peut être rejoué sans risque.
-- Appliqué par `batch db init`, ou par le processus de déploiement habituel du DBA.

CREATE SCHEMA IF NOT EXISTS batch;

CREATE TABLE IF NOT EXISTS batch.job_execution (
    run_id            uuid        PRIMARY KEY,                -- UUID v7 : trié chronologiquement
    job_name          text        NOT NULL,
    parameters_hash   text        NOT NULL,                   -- clé de verrou et de reprise
    parameters        jsonb       NOT NULL,                   -- audit : paramètres effectifs (défauts inclus)
    input_identity    text        NULL,                       -- empreinte de la donnée d'entrée (fichier...)
    trigger_source    text        NOT NULL,                   -- Cli | Quartz | Test
    host_name         text        NOT NULL,
    status            text        NOT NULL,
    started_at        timestamptz NOT NULL,
    ended_at          timestamptz NULL,
    last_heartbeat_at timestamptz NOT NULL,
    restart_of        uuid        NULL REFERENCES batch.job_execution (run_id) ON DELETE SET NULL,
    checkpoint        text        NULL,                       -- dernière position commitée
    read_count        bigint      NOT NULL DEFAULT 0,
    write_count       bigint      NOT NULL DEFAULT 0,
    skip_count        bigint      NOT NULL DEFAULT 0,
    exit_code         integer     NULL,
    message           text        NULL,
    CONSTRAINT ck_job_execution_status CHECK (status IN (
        'Running', 'Succeeded', 'CompletedWithWarnings', 'FunctionalError',
        'TechnicalError', 'Cancelled', 'TimedOut', 'Abandoned'))
);

-- Reprise : « dernière exécution de ce job avec ces paramètres ».
CREATE INDEX IF NOT EXISTS ix_job_execution_latest
    ON batch.job_execution (job_name, parameters_hash, started_at DESC);

-- Détection des zombies : index partiel, minuscule.
CREATE INDEX IF NOT EXISTS ix_job_execution_running
    ON batch.job_execution (job_name)
    WHERE status = 'Running';

-- Purge.
CREATE INDEX IF NOT EXISTS ix_job_execution_ended
    ON batch.job_execution (ended_at)
    WHERE ended_at IS NOT NULL;

-- Supervision : vue prête pour Grafana ou un tableau de bord d'exploitation.
CREATE OR REPLACE VIEW batch.job_last_status AS
SELECT DISTINCT ON (job_name)
       job_name, run_id, status, exit_code, started_at, ended_at,
       read_count, write_count, skip_count, message
FROM batch.job_execution
ORDER BY job_name, started_at DESC;
