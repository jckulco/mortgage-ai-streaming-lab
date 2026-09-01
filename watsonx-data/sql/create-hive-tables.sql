-- ═══════════════════════════════════════════════════════════════════════════
-- Tablas Hive externas sobre los archivos Parquet que escribe Kafka Connect
-- (catálogo Iceberg tipo 'jdbc') en el bucket de watsonx.data.
--
-- Por qué una tabla Hive "externa" y no una tabla Iceberg nativa:
-- La consola de watsonx.data developer edition solo permite crear catálogos
-- Iceberg atados a su propio Hive Metastore gestionado — no hay forma en la
-- UI de registrar un catálogo Iceberg externo (como el JDBC/Postgres que usa
-- Kafka Connect). La forma más simple de hacer los datos consultables desde
-- Presto/watsonx.data sin resolver ese problema de fondo es crear una tabla
-- Hive PLANA apuntando directo a la carpeta de archivos Parquet, ignorando
-- el mecanismo de catálogo Iceberg. Ver docs/ICEBERG_WATSONX.md para el
-- detalle completo de por qué llegamos a esta solución.
--
-- CÓMO CORRER ESTO: pega el bloque completo en el Query workspace de
-- watsonx.data (https://localhost:6443 -> SQL -> Untitled worksheet),
-- selecciona 'presto-01' como Engine, y dale a "Run".
--
-- LIMITACIONES A TENER EN CUENTA:
-- 1. El orden y tipo de columnas debe coincidir EXACTAMENTE con el de los
--    archivos Parquet reales (Presto mapea por POSICIÓN, no por nombre).
--    Si el payload que producen los agentes Python cambia (agregas o quitas
--    un campo), hay que repetir el DROP+CREATE con el nuevo orden.
-- 2. mortgage_decisions es append-only (cada decisión es un evento nuevo),
--    así que esta tabla siempre muestra datos correctos.
-- 3. enriched_mortgage_applications usa upsert-mode en Iceberg (delete files
--    v2). Una tabla Hive externa NO sabe leer delete files, así que vas a
--    ver TODAS las versiones históricas de cada solicitante mezcladas, no
--    solo la última. Para esa tabla, filtra con la query de deduplicación
--    al final de este archivo si necesitas solo el estado más reciente.
-- ═══════════════════════════════════════════════════════════════════════════

-- 1. Schema (una sola vez; falla silenciosamente con IF NOT EXISTS si ya existe)
CREATE SCHEMA IF NOT EXISTS hive_data.mortgage
WITH (location = 's3a://iceberg-bucket/mortgage-hive-schema');


-- 2. mortgage_decisions (append-only, orden real de columnas verificado en
--    producción — ver agents/decision_agent.py línea ~79, dict `output`):
--    applicant_name, applicant_email, decision, interest_rate, email_body
--    (applicant_email fue agregado DESPUÉS de que la tabla ya existía con
--    datos viejos, así que en tablas evolucionadas puede terminar al final
--    del schema físico real — verifica con DESCRIBE si algo no cuadra).

DROP TABLE IF EXISTS hive_data.mortgage.mortgage_decisions;

CREATE TABLE hive_data.mortgage.mortgage_decisions (
    applicant_name  varchar,
    applicant_email varchar,
    decision        varchar,
    interest_rate   double,
    email_body      varchar
)
WITH (
    external_location = 's3a://iceberg-bucket/mortgage/mortgage_decisions/data/',
    format = 'PARQUET'
);

-- Prueba:
-- SELECT * FROM hive_data.mortgage.mortgage_decisions;


-- 3. enriched_mortgage_applications (UPSERT en Iceberg -> puede traer
--    duplicados/versiones viejas mezcladas, ver limitación #3 arriba).
--    Orden real de columnas — ver flink-sql/01-pipeline.sql, SELECT del
--    LEFT JOIN: applicant_name, applicant_email, property_value,
--    loan_amount, annual_income, credit_score, payment_history_status.
--    AJUSTA los tipos/nombres exactos revisando ese archivo si tu pipeline
--    difiere del original.

DROP TABLE IF EXISTS hive_data.mortgage.enriched_mortgage_applications;

CREATE TABLE hive_data.mortgage.enriched_mortgage_applications (
    applicant_name         varchar,
    applicant_email        varchar,
    property_value         double,
    loan_amount            double,
    annual_income           double,
    credit_score            integer,
    payment_history_status  varchar
)
WITH (
    external_location = 's3a://iceberg-bucket/mortgage/enriched_mortgage_applications/data/',
    format = 'PARQUET'
);

-- Prueba con deduplicación (última versión por solicitante, útil porque esta
-- tabla es upsert en el origen y la vista plana puede traer duplicados):
-- SELECT * FROM (
--     SELECT *,
--            ROW_NUMBER() OVER (PARTITION BY applicant_name ORDER BY applicant_name) AS rn
--     FROM hive_data.mortgage.enriched_mortgage_applications
-- ) WHERE rn = 1;
