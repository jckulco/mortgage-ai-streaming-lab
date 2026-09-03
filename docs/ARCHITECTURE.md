# Arquitectura

## Diagrama de componentes

```
┌──────────────┐      ┌────────────────────┐
│  producer/   │─────▶│ mortgage_applications│ (topic)
│ submit_app.py│      └────────────────────┘
└──────────────┘                │
                                 │
┌───────────────┐   ┌───────────▼───────────┐   ┌──────────────────┐
│ postgres-      │──▶│   Kafka Connect (JDBC) │──▶│ raw-credit_scores │
│  mortgage      │   │                        │   │ raw-payment_history│(topics)
└───────────────┘   └────────────────────────┘   └──────────────────┘
                                 │                          │
                                 ▼                          ▼
                     ┌───────────────────────────────────────────┐
                     │           Apache Flink (SQL job)            │
                     │  LEFT JOIN mortgage_applications             │
                     │           x credit_scores x payment_history │
                     └───────────────────┬───────────────────────┘
                                          ▼
                          ┌───────────────────────────────┐
                          │ enriched_mortgage_applications │ (topic)
                          └───────────────┬───────────────┘
                                          ▼
                          ┌───────────────────────────────┐
                          │  ai-risk-agent (Python + LLM)  │──▶ OpenLineage event
                          └───────────────┬───────────────┘
                                          ▼
                          ┌───────────────────────────────┐
                          │  mortgage_validated_apps        │ (topic)
                          └───────────────┬───────────────┘
                                          ▼
                          ┌───────────────────────────────┐
                          │ ai-decision-agent (Python + LLM)│──▶ OpenLineage event
                          └───────────────┬───────────────┘
                                          ▼
                          ┌───────────────────────────────┐
                          │      mortgage_decisions         │ (topic)
                          └───────────────┬───────────────┘
                                          ▼
                          ┌───────────────────────────────┐
                          │ ai-email-notifier (Python)      │
                          └───────────────┬───────────────┘
                                          ▼
                          ┌───────────────────────────────┐
                          │            Mailpit               │ (SMTP falso)
                          └───────────────────────────────┘

                Kafbat UI ─── observa todos los topics/schemas/connectors
                Marquez  ─── observa el lineage emitido por los agentes IA
                Flink UI ─── observa el job de enriquecimiento en ejecución
                Mailpit  ─── bandeja web (http://localhost:8025) con los
                              correos de aprobación/rechazo, sin salir a
                              Internet ni requerir credenciales reales

              enriched_mortgage_applications ┐
              mortgage_decisions             ┴──▶ Kafka Connect (Iceberg sink,
                                                    catálogo jdbc/Postgres)
                                                    ──▶ MinIO de watsonx.data
                                                    developer edition
                                                    (opcional, ver
                                                    docs/ICEBERG_WATSONX.md)
```

## Por qué estas decisiones

### Flink en vez de ksqlDB

El lab original de Confluent Cloud usa Flink SQL gestionado para el
enriquecimiento. Aquí se usa **Apache Flink OSS** (JobManager + TaskManager +
SQL Client) para mantener la misma sintaxis y el mismo modelo mental
(`CREATE TABLE ... WITH ('connector'='kafka', ...)`, joins en streaming),
en vez de traducir todo a ksqlDB. Esto también deja la puerta abierta a usar
UDFs de Flink si en el futuro quieres mover la llamada al LLM *dentro* del
SQL (ver "Trabajo futuro" más abajo).

### JSON en vez de Avro entre Connect y Flink

Los conectores JDBC están configurados con `JsonConverter` (no
`AvroConverter` + Schema Registry) para los topics `raw-credit_scores` y
`raw-payment_history`. Esto evita tener que añadir el conector
`flink-sql-avro-confluent-registry` a la imagen de Flink solo para este
ejercicio. Si tu caso de uso real necesita Avro con evolución de esquemas,
es un cambio de configuración, no de arquitectura: cambia el `value.converter`
del conector y el `'format' = 'avro-confluent'` en las tablas Flink
correspondientes.

### Los agentes de IA son microservicios, no UDFs de Flink

Confluent Cloud permite invocar modelos de IA directamente desde una
sentencia Flink SQL (`ML_PREDICT` / integraciones de agentes gestionadas).
Flink OSS no trae eso de fábrica — se podría lograr escribiendo una UDF en
Python/Java que haga la llamada HTTP al LLM y registrándola con
`CREATE FUNCTION`, pero eso añade complejidad de empaquetado (JARs,
dependencias de red dentro del proceso de Flink, manejo de reintentos) que
no aporta claridad pedagógica al ejercicio. Un microservicio Python
consumidor/productor de Kafka es más simple de leer, depurar y modificar,
y es un patrón igualmente válido en producción (muchos pipelines reales
combinan Flink para transformación de datos y microservicios para llamadas
a servicios externos con lógica de reintento/rate-limiting propia).

### OpenLineage se emite desde los agentes, no desde Flink

Existe un listener de OpenLineage para Flink (`openlineage-flink`) que
emitiría eventos de lineage automáticamente por cada job. No se incluyó en
este repo por dos razones: (1) requiere fijar versiones compatibles exactas
entre Flink, el listener y el runtime, lo cual es frágil de mantener en un
proyecto de referencia; y (2) el enriquecimiento por sí solo (joins de
topics) es la parte menos interesante de trazar — el valor real de mostrar
lineage en esta demo está en poder ver **qué modelo/versión de agente
IA tomó cada decisión**, que es justo lo que emiten `risk_agent.py` y
`decision_agent.py` vía `openlineage-python`.

### Mailpit en vez de un proveedor SMTP real

El `ai-email-notifier` consume `mortgage_decisions` y envía el correo de
aprobación/rechazo (usando el `email_body` que ya redacta
`ai-decision-agent`) vía SMTP a **Mailpit**, un servidor SMTP falso que
corre como un contenedor más dentro del mismo `docker-compose.yml`. Mailpit
captura cualquier correo enviado y lo muestra en una bandeja web
(`http://localhost:8025`), sin salir nunca a Internet ni requerir
credenciales de un proveedor real (Gmail, SES, SendGrid, etc.).

Esto mantiene el lab 100% local y reproducible sin depender de una cuenta de
correo externa. El código del agente usa `smtplib` (librería estándar de
Python, sin dependencias nuevas) y las variables `SMTP_HOST`/`SMTP_PORT` en
`.env` — para apuntar a un SMTP real en lugar de Mailpit, solo hay que
cambiar esas variables; el código no cambia.

## Trabajo futuro (ideas para extender el proyecto)

- Añadir el listener `openlineage-flink` al job de enriquecimiento para
  tener lineage de extremo a extremo (Postgres → Flink → topics), no solo
  desde los agentes en adelante.
- Mover la lógica de los agentes a UDFs de Flink (`CREATE FUNCTION`) para
  acercarse aún más a la experiencia de Flink gestionado de Confluent Cloud.
- Añadir cifrado a nivel de campo para `credit_score` / `debt_to_income`
  (mencionado como buena práctica en el lab original).
- Usar un proveedor SMTP real (o un servidor MCP propio) en vez de Mailpit,
  para notificaciones que de verdad lleguen a una bandeja de entrada externa
  — es solo cambiar variables en `.env`, ver sección de arriba.
- Autenticación/TLS en todos los componentes si el repo se usa como base
  para algo más que un lab local.
