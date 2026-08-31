-- ═══════════════════════════════════════════════════════════════════════════
-- 02-inspect.sql
-- NO se ejecuta automáticamente en `make start`. Es para pegar manualmente
-- en el SQL Client interactivo (`make flink-sql`) y curiosear los topics
-- que producen los agentes de IA (que no son jobs de Flink, son
-- microservicios Python — ver agents/).
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS mortgage_validated_apps (
    applicant_name  STRING,
    risk_level      STRING,
    risk_summary    STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'mortgage_validated_apps',
    'properties.bootstrap.servers' = 'broker:29092',
    'properties.group.id' = 'flink-inspect-validated',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true',
    'scan.startup.mode' = 'earliest-offset'
);

CREATE TABLE IF NOT EXISTS mortgage_decisions (
    applicant_name  STRING,
    decision        STRING,
    interest_rate   DOUBLE,
    email_body      STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'mortgage_decisions',
    'properties.bootstrap.servers' = 'broker:29092',
    'properties.group.id' = 'flink-inspect-decisions',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true',
    'scan.startup.mode' = 'earliest-offset'
);

-- Ejemplos:
-- SELECT * FROM enriched_mortgage_applications;
-- SELECT * FROM mortgage_validated_apps;
-- SELECT * FROM mortgage_decisions WHERE applicant_name = 'John Doe';
