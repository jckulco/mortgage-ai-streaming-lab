# Kafka Connect → Apache Iceberg → watsonx.data developer edition

Este documento describe la integración completa y ya probada en producción:
Kafka Connect escribe `enriched_mortgage_applications` y `mortgage_decisions`
como tablas Iceberg reales en el **mismo MinIO** que usa watsonx.data
developer edition, y watsonx.data las consulta con SQL vía Presto.

## Arquitectura final

```
                    (Kafka, dentro de mortgage-lab-network)
mortgage_decisions ──┐
                      ├──▶ Kafka Connect ──▶ Iceberg Sink Connector
enriched_mortgage_   ┘         │                     │
applications                   │        catálogo: JDBC (Postgres propio
                                │        del proyecto, postgres-mortgage)
                                │        storage:  MinIO de watsonx.data
                                ▼
                    host.docker.internal:9000 (MinIO S3)
                    ← expuesto con 'kubectl port-forward' + auto-reinicio
                                │
                                ▼
              Bucket iceberg-bucket (mismo storage físico de watsonx.data)
                                │
                                ▼
        watsonx.data / Presto: tabla Hive EXTERNA sobre los Parquet
        (hive_data.mortgage.mortgage_decisions, etc.) — consultable con SQL
```

## Dos decisiones de diseño que vale la pena entender

### 1. Catálogo JDBC (Postgres), no Hive Metastore

Investigamos usar el Hive Metastore que trae watsonx.data
(`ibm-lh-mds-thrift-svc`), que expone dos puertos: `8381` (HTTPS + auth
básica, usado internamente por Presto) y `8380` (etiquetado "thrift" en el
Service de Kubernetes). Probamos el `8380` esperando que fuera un endpoint
Hive Metastore plano estándar — **no lo es**: el servidor acepta la conexión
TCP pero cierra el socket a medio protocolo (`Socket is closed by peer`) en
cuanto el cliente Hive intenta una llamada real. No hay evidencia de que ese
puerto sirva un protocolo Hive Metastore utilizable por clientes externos en
esta versión del developer edition.

En su lugar, el sink usa **`iceberg.catalog.type = jdbc`**, respaldado en el
Postgres que el propio proyecto ya trae (`postgres-mortgage`). Este catálogo
no depende en absoluto de infraestructura de watsonx.data — solo el
**storage** (MinIO) es compartido.

### 2. Tabla Hive externa en watsonx.data, no un catálogo Iceberg propio

La consola de watsonx.data developer edition (Infrastructure manager → Add
component → catálogo) **siempre** crea catálogos Iceberg atados a su propio
Hive Metastore gestionado — no hay ningún campo en la UI para registrar un
catálogo externo (JDBC, REST, etc.). Por eso, aunque los archivos Parquet
están físicamente en el bucket que watsonx.data usa, Presto no los "ve" como
tablas hasta que se los indicamos explícitamente.

La solución que funciona: usar el catálogo `hive_data` (tipo Apache Hive,
que sí trae watsonx.data por defecto) para crear una **tabla externa** que
apunta directo a la carpeta de archivos Parquet, sin pasar por ningún
mecanismo de catálogo Iceberg. Es SQL puro, corrido una vez en el Query
workspace — ver la sección de abajo.

**Limitación a aceptar**: esa tabla Hive es una foto fija del esquema. Si el
payload que producen los agentes Python cambia de forma (agregas/quitas un
campo), hay que repetir el `DROP TABLE` + `CREATE TABLE` con el nuevo orden
de columnas — Presto mapea por posición, no por nombre, en tablas Hive
externas sobre Parquet plano. Además, `enriched_mortgage_applications` usa
upsert-mode en Iceberg (borra versiones viejas con "delete files"), que una
tabla Hive externa no sabe interpretar — vas a ver todas las versiones
históricas mezcladas ahí, no solo la más reciente (usa la query de
deduplicación incluida en `watsonx-data/sql/create-hive-tables.sql`).

## Prerrequisito en la VM

`scripts/register-iceberg-connectors.sh` usa `envsubst` (paquete `gettext`):
```bash
sudo dnf install -y gettext
```

El pipeline de Python (`producer/submit_application.py`) necesita:
```bash
sudo dnf install -y python3-pip
pip3 install confluent_kafka
```
(Si `confluent_kafka` falla al compilar, instala primero:
`sudo dnf install -y gcc librdkafka-devel python3-devel`)

## Paso a paso

### 1. Exponer MinIO de watsonx.data (con auto-reinicio)

```bash
make iceberg-expose-minio
```

Este script (`watsonx-data/scripts/get-iceberg-connection-info.sh`) detecta
el namespace de Kubernetes (`wxd`/`spark`), y deja corriendo en background un
`kubectl port-forward` **con un bucle de auto-reinicio**: un port-forward
normal se cae solo cada cierto tiempo (timeout de inactividad, reinicio del
pod), y sin el watchdog vuelves a ver `Connection refused` en los conectores
al rato. Imprime las credenciales S3 (en developer edition suelen ser
literalmente `dummyvalue`/`dummyvalue`).

Para detenerlo por completo más adelante:
```bash
pkill -f 'port-forward-watchdog-minio'
```

### 2. Completar `.env`

```bash
ICEBERG_ENABLED=true
ICEBERG_NAMESPACE=mortgage
ICEBERG_S3_HOST=host.docker.internal
ICEBERG_S3_PORT=9000
ICEBERG_S3_ACCESS_KEY=dummyvalue
ICEBERG_S3_SECRET_KEY=dummyvalue
ICEBERG_BUCKET=iceberg-bucket
```

`ICEBERG_BUCKET=iceberg-bucket` reutiliza el bucket que watsonx.data ya trae
por defecto (visible en Infrastructure manager → Storage) — no hace falta
crear uno nuevo.

### 3. Compilar el conector Iceberg (una sola vez)

Apache Iceberg **no publica un .jar/.zip precompilado** del
`kafka-connect-runtime` — hay que compilarlo desde el código fuente:

```bash
make iceberg-connector-build
```

Tarda ~3-5 minutos la primera vez (compila Apache Iceberg desde GitHub, tag
`apache-iceberg-1.9.0`) y reinicia el contenedor `connect` para instalar el
plugin. `connect-init.sh` también copia automáticamente el driver de
PostgreSQL (ya presente en el conector JDBC de Confluent) al plugin de
Iceberg — sin este paso, el catálogo `jdbc` falla con
`No suitable driver found`.

### 4. Registrar los conectores sink

```bash
make iceberg-connectors
```

Registra `iceberg-enriched-sink` (upsert-mode) e `iceberg-decisions-sink`
(append-only) vía `PUT /connectors/<name>/config`. Verifica que ambos queden
`RUNNING` (no `FAILED`):

```bash
make iceberg-status
```

Si algo falla, `curl -s http://localhost:8083/connectors/<nombre>/status`
trae el stacktrace completo — los errores más comunes ya los mapeamos:

| Síntoma | Causa | Solución |
|---|---|---|
| `Connection refused` al escribir a MinIO | El port-forward se cayó | `make iceberg-expose-minio` de nuevo (o confirma que el watchdog sigue vivo: `ps aux \| grep port-forward-watchdog`) |
| `No suitable driver found for jdbc:postgresql` | Falta el driver Postgres en el plugin | Ya automatizado en `connect-init.sh`; si persiste, `docker compose restart connect` |
| `Unrecognized token 'X': was expecting JSON...` en la key | La key del topic es texto plano, no JSON | Usa `key.converter: StringConverter` (ya así en `mortgage_decisions`) |

### 5. Generar datos y verificar en MinIO

```bash
make producer
```

Espera ~15-20s (`iceberg.control.commit.interval-ms=10000`) y revisa en la
consola de MinIO de watsonx.data (`http://localhost:9001`,
`dummyvalue`/`dummyvalue`) que aparezcan archivos en
`iceberg-bucket/mortgage/mortgage_decisions/data/` y
`iceberg-bucket/mortgage/enriched_mortgage_applications/data/`.

### 6. Consultar desde watsonx.data con SQL

```bash
make iceberg-hive-sql
```

Copia el SQL que imprime (o ábrelo directo en
`watsonx-data/sql/create-hive-tables.sql`) y pégalo en el **Query
workspace** de watsonx.data (`https://localhost:6443` → ícono SQL), con
Engine `presto-01`. Crea el schema y las dos tablas externas, listas para:

```sql
SELECT * FROM hive_data.mortgage.mortgage_decisions;
```

## Notas y limitaciones

- **Recursos**: sumar Kafka+Flink+Connect+Marquez y watsonx.data en
  simultáneo en la misma VM de 31GB puede quedar apretado. Revisa consumo
  real con `make health` y `kubectl top pods -n <namespace>` antes de dejar
  todo prendido a la vez.
- **Reinicios de la VM**: el `kubectl port-forward` (incluso con el
  watchdog) no sobrevive a un reinicio de la VM — corre `make
  iceberg-expose-minio` de nuevo después de un reboot.
- **Evolución de esquema**: si cambias los campos que producen
  `risk_agent.py` / `decision_agent.py` / `01-pipeline.sql`, repite el
  `DROP TABLE` + `CREATE TABLE` de `watsonx-data/sql/create-hive-tables.sql`
  con el nuevo orden de columnas exacto.
- **`enriched_mortgage_applications` trae duplicados** por el tema de
  upsert/delete-files explicado arriba — usa siempre la variante con
  `ROW_NUMBER() OVER (...)` de ese archivo SQL para la versión más reciente
  por solicitante.
