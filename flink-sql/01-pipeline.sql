-- ═══════════════════════════════════════════════════════════════════════════
-- 01-pipeline.sql
-- Fuentes + enriquecimiento, en un único script.
--
-- Importante: el catálogo del SQL Client de Flink es en memoria y vive solo
-- durante la sesión. Por eso las tablas fuente y el INSERT INTO que arranca
-- el job de streaming deben declararse en el mismo archivo/sesión — una vez
-- que el INSERT INTO se envía al cluster, el job sigue corriendo aunque el
-- cliente se desconecte (el job vive en el JobManager, no en el cliente).
--
-- Ejecutar con:
--   bin/sql-client.sh -f /opt/flink/sql-scripts/01-pipeline.sql
-- ═══════════════════════════════════════════════════════════════════════════

SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'streaming';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';

-- ── Fuentes ──────────────────────────────────────────────────────────────

-- Solicitudes de hipoteca (publicadas por producer/submit_application.py)
CREATE TABLE IF NOT EXISTS mortgage_applications (
    applicant_name  STRING,
    property_value  DOUBLE,
    loan_amount     DOUBLE,
    annual_income   DOUBLE,
    submitted_at    BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'mortgage_applications',
    'properties.bootstrap.servers' = 'broker:29092',
    'properties.group.id' = 'flink-mortgage-apps',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true',
    'scan.startup.mode' = 'earliest-offset'
);

-- Score crediticio, vía Kafka Connect JDBC (topic: raw-credit_scores).
-- Se declara como upsert-kafka (última versión conocida por applicant_name),
-- que es el equivalente en Flink al "table" de la tabla Postgres original.
CREATE TABLE IF NOT EXISTS credit_scores (
    applicant_name  STRING,
    credit_score    INT,
    debt_to_income  DOUBLE,
    PRIMARY KEY (applicant_name) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'raw-credit_scores',
    'properties.bootstrap.servers' = 'broker:29092',
    'key.format' = 'json',
    'value.format' = 'json',
    'value.json.ignore-parse-errors' = 'true'
);

-- Historial de pagos (topic: raw-payment_history)
CREATE TABLE IF NOT EXISTS payment_history (
    applicant_name    STRING,
    on_time_payments  INT,
    late_payments     INT,
    PRIMARY KEY (applicant_name) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'raw-payment_history',
    'properties.bootstrap.servers' = 'broker:29092',
    'key.format' = 'json',
    'value.format' = 'json',
    'value.json.ignore-parse-errors' = 'true'
);

-- ── Sink del enriquecimiento ────────────────────────────────────────────
-- Equivalente al topic enriched_mortgage_applications del lab original.
CREATE TABLE IF NOT EXISTS enriched_mortgage_applications (
    applicant_name  STRING,
    property_value  DOUBLE,
    loan_amount     DOUBLE,
    annual_income   DOUBLE,
    credit_score    INT,
    debt_to_income  DOUBLE,
    on_time_payments INT,
    late_payments    INT
) WITH (
    'connector' = 'kafka',
    'topic' = 'enriched_mortgage_applications',
    'properties.bootstrap.servers' = 'broker:29092',
    'format' = 'json'
);

-- ── Job de streaming: doble LEFT JOIN (equivalente a los dos statements
--    Flink SQL encadenados del lab original de Confluent Cloud).
--    Es un join regular stream-tabla: credit_scores y payment_history son
--    tablas dinámicas (changelog) alimentadas por upsert-kafka, así que
--    Flink las mantiene como estado y las actualiza automáticamente cuando
--    llegan nuevos valores desde el conector JDBC. No hace falta sintaxis
--    de "temporal table" (FOR SYSTEM_TIME AS OF) para este caso. ─────────
INSERT INTO enriched_mortgage_applications
SELECT
    a.applicant_name,
    a.property_value,
    a.loan_amount,
    a.annual_income,
    c.credit_score,
    c.debt_to_income,
    p.on_time_payments,
    p.late_payments
FROM mortgage_applications a
LEFT JOIN credit_scores c ON a.applicant_name = c.applicant_name
LEFT JOIN payment_history p ON a.applicant_name = p.applicant_name;
