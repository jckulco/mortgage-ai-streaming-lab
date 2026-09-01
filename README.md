# Mortgage AI Streaming Lab

Pipeline de streaming de extremo a extremo, 100% open source, que reproduce
localmente un flujo de **decisión hipotecaria asistida por IA**: ingesta de
solicitudes, enriquecimiento en tiempo real con Apache Flink, dos agentes de
IA (evaluación de riesgo y decisión final) y trazabilidad de datos con
OpenLineage/Marquez.

Inspirado en el lab `[Confluent L3]: AI-Driven Mortgage Decisioning with
Confluent` de Confluent Cloud, pero construido enteramente sobre componentes
open source que puedes correr en tu laptop con `docker compose`.

[![CI](https://github.com/OWNER/mortgage-ai-streaming-lab/actions/workflows/ci.yml/badge.svg)](.github/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## Stack

| Componente | Rol |
|---|---|
| **Apache Kafka** (KRaft, sin ZooKeeper) | Backbone de streaming |
| **Kafka Connect** (conector JDBC) | Ingesta de credit score / historial de pagos desde Postgres |
| **Apache Flink** (JobManager + TaskManager + SQL Client) | Enriquecimiento en tiempo real (joins) |
| **Kafbat UI** | Inspección de topics, schemas, conectores |
| **Marquez + OpenLineage** | Lineage: qué agente de IA tomó qué decisión y con qué datos |
| **Agentes de IA (Python)** | Evaluación de riesgo y decisión final, vía cualquier LLM compatible con la API de OpenAI |
| **PostgreSQL** | Fuente de datos de crédito y pagos del ejercicio |
| **watsonx.data developer edition** *(opcional)* | Lakehouse local (Apache Iceberg) — ver `watsonx-data/README.md` |

Ver [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) para el diagrama completo
y el razonamiento detrás de cada decisión de diseño.

---

## Requisitos

- Docker Engine 20.10+ y Docker Compose v2
- ~8 GB RAM libres, 10 GB de disco
- Una API key de watsonx.ai (IBM Cloud) **o** de un LLM compatible con
  `/v1/chat/completions` (OpenAI, Azure OpenAI, Ollama local...) — ver
  `.env.example`, sección `LLM_PROVIDER`
- Python 3.9+ (solo para correr `producer/submit_application.py` desde tu
  máquina; no es necesario dentro de los contenedores)

---

## Arranque rápido

```bash
git clone https://github.com/OWNER/mortgage-ai-streaming-lab.git
cd mortgage-ai-streaming-lab

cp .env.example .env
# Edita .env y coloca tu LLM_API_KEY

make start
```

`make start` construye las imágenes (Flink personalizado + agentes),
levanta todo el stack, registra los conectores JDBC y despliega el job de
enriquecimiento en Flink automáticamente.

Interfaces disponibles al terminar:

| Interfaz | URL |
|---|---|
| **Formulario de solicitud (frontend)** | http://localhost:5050 |
| Kafbat UI | http://localhost:8080 |
| Flink UI (jobs en ejecución) | http://localhost:8082 |
| Marquez (grafo de lineage) | http://localhost:3000 |
| Schema Registry | http://localhost:8081 |
| Kafka Connect REST | http://localhost:8083 |

---

## Probar el pipeline completo

**Opción A — formulario web** (recomendado): abre http://localhost:5050,
llena el formulario "Mortgage Application" y presiona **Submit Application**.

**Opción B — línea de comandos**:
```bash
# Envía una solicitud de hipoteca (John Doe ya tiene datos de crédito
# precargados en Postgres, ver sql/init-mortgage-db.sql)
make producer

# O manualmente con otro solicitante:
python3 producer/submit_application.py --name "Emmet Wisoky" \
  --property-value 180000 --loan-amount 160000 --annual-income 75000
```

En unos segundos:

```bash
docker logs -f ai-risk-agent       # ves la evaluación de riesgo
docker logs -f ai-decision-agent   # ves la decisión final
```

O revisa visualmente:
- **Kafbat UI** → Topics → `mortgage_decisions` → Messages
- **Flink UI** → Running Jobs → el job de enriquecimiento en vivo
- **Marquez** → Namespace `mortgage-lab` → grafo con `risk-assessment-agent`
  y `decision-agent` como nodos, con los topics de entrada/salida de cada uno

---

## Comandos útiles

```bash
make health            # diagnóstico de todos los servicios
make flink-sql         # abre el SQL Client de Flink interactivo
make topics-list       # lista los topics de Kafka
make connectors-list   # lista los conectores registrados
make logs              # logs en vivo de todo el stack
make stop              # detiene los contenedores (conserva datos)
make clean             # elimina contenedores + datos persistentes (irreversible)
```

Dentro del SQL Client de Flink (`make flink-sql`), puedes pegar las queries
de [`flink-sql/02-inspect.sql`](flink-sql/02-inspect.sql) para consultar los
topics de salida de los agentes directamente en SQL.

---

## Estructura del proyecto

```
mortgage-ai-streaming-lab/
├── docker-compose.yml            # stack completo
├── .env.example                  # plantilla de configuración
├── Makefile                      # comandos rápidos
├── flink/
│   └── Dockerfile                # imagen de Flink + conector Kafka
├── flink-sql/
│   ├── 01-pipeline.sql           # fuentes + job de enriquecimiento (auto-desplegado)
│   └── 02-inspect.sql            # queries manuales de inspección
├── connectors/
│   ├── credit-score-source.json
│   └── payment-history-source.json
├── sql/
│   └── init-mortgage-db.sql      # datos de ejemplo (credit_scores, payment_history)
├── agents/
│   ├── common.py                 # cliente Kafka + LLM + emisor OpenLineage
│   ├── risk_agent.py             # agente 1: evaluación de riesgo
│   ├── decision_agent.py         # agente 2: decisión final
│   └── Dockerfile
├── producer/
│   └── submit_application.py     # simula la solicitud vía CLI (alternativa al frontend)
├── frontend/
│   ├── app.py                    # formulario web de solicitud (Flask)
│   ├── templates/index.html
│   └── Dockerfile
├── watsonx-data/                 # OPCIONAL: lakehouse local, ver watsonx-data/README.md
│   ├── installer/                # coloca aquí el .tar descargado de IBM (no versionado)
│   └── scripts/
│       ├── setup-watsonx-data.sh
│       ├── status-watsonx-data.sh
│       └── teardown-watsonx-data.sh
├── scripts/
│   ├── start.sh                  # arranque + configuración automática
│   ├── health-check.sh
│   └── connect-init.sh
├── docs/
│   └── ARCHITECTURE.md           # diagrama y decisiones de diseño
└── .github/workflows/ci.yml      # valida compose, JSON y build de imágenes
```

---

## Seguridad

⚠️ Este stack **no** tiene autenticación, TLS ni ACLs. Está diseñado
exclusivamente para desarrollo local, pruebas y demos. No lo expongas a
redes públicas ni lo uses como base directa para producción sin añadir
las capas de seguridad correspondientes (SASL/SSL, RBAC en Kafbat UI,
secrets management para las API keys de LLM, etc.).

---

## Solución de problemas

| Síntoma | Causa | Solución |
|---|---|---|
| `connect` queda `unhealthy` / reinicia en bucle, log dice `componentDir ... not a writeable path` | `./data/connect-plugins` pertenece a `root` en el host, pero el contenedor corre como uid `1000` | `sudo chown -R 1000:1000 data/connect-plugins && docker compose up -d connect` — `scripts/start.sh` ya lo corrige automáticamente en instalaciones nuevas |
| `Port XXXX already in use` | Otro proceso usa ese puerto | Cambia el puerto correspondiente en `.env` (`FLINK_UI_PORT`, `KAFKA_UI_PORT`, etc.) |
| El job de Flink no aparece en la Flink UI | El SQL Client falló silenciosamente | `docker compose exec flink-sql-client bin/sql-client.sh -f /opt/flink/sql-scripts/01-pipeline.sql` manualmente para ver el error completo |
| Los agentes no producen nada en `mortgage_validated_apps` / `mortgage_decisions` | Falla la llamada al LLM (credenciales, `LLM_PROVIDER` mal configurado) | `docker logs -f ai-risk-agent` — revisa el mensaje de error específico (401, project_id inválido, etc.) |
| `broker` en `Restarting` en loop | Datos corruptos de un intento anterior | `make clean && make start` (⚠️ borra todos los datos) |

---

## Créditos y licencia

Inspirado en el lab de Confluent Cloud "AI-Driven Mortgage Decisioning".
Este proyecto es una implementación independiente sobre componentes 100%
open source (Apache Kafka, Apache Flink, OpenLineage/Marquez, Kafbat UI) y
no está afiliado a Confluent Inc.

Licencia [MIT](LICENSE).
