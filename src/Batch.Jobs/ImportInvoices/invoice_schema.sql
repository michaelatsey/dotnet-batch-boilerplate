-- Table cible du job import-invoices. Idempotent.
CREATE SCHEMA IF NOT EXISTS billing;

CREATE TABLE IF NOT EXISTS billing.invoice (
    invoice_number text          PRIMARY KEY,               -- clé naturelle : rend l'import rejouable
    customer_id    text          NOT NULL,
    issue_date     date          NOT NULL,
    amount         numeric(14,2) NOT NULL CHECK (amount >= 0),
    currency       text          NOT NULL CHECK (char_length(currency) = 3),
    source_run_id  uuid          NOT NULL,                  -- traçabilité : quel run a écrit la ligne
    created_at     timestamptz   NOT NULL DEFAULT now(),
    updated_at     timestamptz   NOT NULL DEFAULT now()
);
