# Kafka Connect → Apache Iceberg → watsonx.data developer edition

Este documento describe cómo llevar `enriched_mortgage_applications` y
`mortgage_decisions` de Kafka a tablas Iceberg, escribiendo directamente en
el **mismo MinIO** que ya trae watsonx.data developer edition, usando un
**sink connector de Kafka Connect** (no se toca Flink ni los agentes).

## Arquitectura

```
                         (Kafka, dentro de mortgage-lab-network)
mortgage_decisions ──┐
                      ├──▶ Kafka Connect ──▶ Iceberg Sink Connector
enriched_mortgage_   ┘         │                     │
applications                   │                     ▼
                                │      catálogo: Hive Metastore (HMS)
                                │      storage:  MinIO de watsonx.data
                                │      (namespace 'wxd' en KIND, red aparte)
                                ▼
                    host.docker.internal:9083 (HMS)
                    host.docker.internal:9000 (MinIO S3)
                    ← expuestos con 'kubectl port-forward'
```

watsonx.data developer edition corre en su **propio cluster Kubernetes
(KIND)**, en una red Docker distinta a `mortgage-lab-network`. Por eso el
contenedor `connect` no puede resolver los Services de Kubernetes
directamente — hay que exponer el Hive Metastore y el MinIO **en el host**
de la VM (con `kubectl port-forward`) y que Connect les llegue vía
`host.docker.internal` (ya configurado en `docker-compose.yml` con
`extra_hosts: host.docker.internal:host-gateway`).

## Por qué Hive Metastore y no un catálogo REST propio

watsonx.data developer edition usa **Hive Metastore (HMS)** como catálogo de
Iceberg por defecto (no expone un catálogo REST de Iceberg independiente).
Por eso el sink connector se configura con `iceberg.catalog.type = hive`
apuntando al HMS de watsonx.data — así las tablas que escribe Kafka Connect
quedan registradas en el mismo catálogo que usa Presto dentro de
watsonx.data, y aparecen automáticamente en la consola sin pasos extra de
registro de catálogo.

## Prerrequisito en la VM

`scripts/register-iceberg-connectors.sh` usa `envsubst` (paquete `gettext`,
normalmente ya viene en RHEL 9, pero si falta):

```bash
sudo dnf install -y gettext
```

## Paso a paso

### 1. Exponer HMS y MinIO de watsonx.data, y obtener credenciales

```bash
bash watsonx-data/scripts/get-iceberg-connection-info.sh
```

Esto detecta el namespace de Kubernetes (`wxd` o `spark`), deja corriendo en
background dos `kubectl port-forward` (HMS en `:9083`, MinIO S3 en `:9000`
del host), e imprime `LH_S3_ACCESS_KEY` / `LH_S3_SECRET_KEY`.

Si la detección automática de credenciales falla (varía según versión del
instalador), consíguelas manualmente desde la consola de watsonx.data
(`https://localhost:6443` → Infrastructure manager → tu bucket →
credenciales) o con `kubectl -n <namespace> exec <pod> -- printenv | grep LH_S3`.

### 2. Crear el bucket destino en el MinIO de watsonx.data

Desde la consola MinIO de watsonx.data (o `mc mb`), crea un bucket — por
defecto el proyecto espera `mortgage-lab` (configurable con `ICEBERG_BUCKET`).

### 3. Completar `.env`

```bash
ICEBERG_ENABLED=true
ICEBERG_NAMESPACE=mortgage
ICEBERG_HMS_HOST=host.docker.internal
ICEBERG_HMS_PORT=9083
ICEBERG_S3_HOST=host.docker.internal
ICEBERG_S3_PORT=9000
ICEBERG_S3_ACCESS_KEY=<de get-iceberg-connection-info.sh>
ICEBERG_S3_SECRET_KEY=<de get-iceberg-connection-info.sh>
ICEBERG_BUCKET=mortgage-lab
```

### 4. Compilar el conector Iceberg (una sola vez)

Apache Iceberg **no publica un .jar/.zip precompilado** del
`kafka-connect-runtime` (a diferencia del JDBC connector, que instalan con
`confluent-hub`) — hay que compilarlo desde el código fuente. Este paso lo
automatiza un contenedor Gradle efímero, no necesitas Java instalado en la VM:

```bash
make iceberg-connector-build
```

Tarda ~5-10 minutos la primera vez (compila Apache Iceberg desde GitHub) y
reinicia el contenedor `connect` para que `connect-init.sh` instale el
plugin resultante. Es idempotente: si ya existe el `.zip` en
`connectors/build/`, no vuelve a compilar.

### 5. Registrar los conectores sink

```bash
make iceberg-connectors
```

Esto aplica `connectors/iceberg-enriched-sink.json.template` y
`connectors/iceberg-decisions-sink.json.template` (con las variables de
`.env` sustituidas) vía `PUT /connectors/<name>/config`.

### 6. Verificar

```bash
make iceberg-status
```

Y con una solicitud de prueba (`make producer`), en 10-15 segundos
(`iceberg.control.commit.interval-ms`) deberían aparecer archivos Parquet +
metadata Iceberg en el bucket MinIO de watsonx.data, y las tablas
`mortgage.enriched_mortgage_applications` y `mortgage.mortgage_decisions`
visibles en la consola de watsonx.data (bajo el catálogo Hive por defecto,
namespace `mortgage`) — listas para consultarlas con Presto desde ahí mismo.

## Notas y limitaciones

- **`enriched_mortgage_applications` se registra en modo upsert** (clave
  `applicant_name`), porque el topic de origen es `upsert-kafka` en Flink —
  así la tabla Iceberg refleja el último estado por solicitante, no
  duplicados. `mortgage_decisions` se escribe en modo *append* (cada
  decisión es un evento nuevo).
- **Recursos**: sumar Kafka+Flink+Connect+Marquez y watsonx.data en
  simultáneo en la misma VM de 31GB puede quedar apretado. Antes de dejar
  todo prendido a la vez, revisa consumo real con `make health` y
  `kubectl top pods -n <namespace>`.
- Si cambian los puertos de `port-forward` (por ejemplo porque ya usas 9083
  o 9000 para otra cosa), ajusta `HMS_LOCAL_PORT` / `S3_LOCAL_PORT` como
  variables de entorno al llamar `get-iceberg-connection-info.sh`, y refleja
  el mismo valor en `ICEBERG_HMS_PORT` / `ICEBERG_S3_PORT` en `.env`.
- Los `port-forward` de `kubectl` no sobreviven a un reinicio de la VM —
  hay que volver a correr `get-iceberg-connection-info.sh` después de un
  reboot (o convertirlo en un servicio systemd si lo quieres persistente).
