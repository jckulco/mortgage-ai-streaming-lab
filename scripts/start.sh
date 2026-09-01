#!/usr/bin/env bash
# Arranca el stack completo, crea los conectores y despliega los joins de
# enriquecimiento en Flink. Idempotente: se puede correr varias veces.
set -euo pipefail

cd "$(dirname "$0")/.."

if [ ! -f .env ]; then
  echo ">> No existe .env, copiando desde .env.example"
  cp .env.example .env
  echo "   Edita .env (sobre todo LLM_API_KEY) y vuelve a correr este script."
fi

mkdir -p data/kafka-data data/connect-plugins data/postgres-mortgage data/marquez-db

# El contenedor de Kafka Connect corre como usuario no-root (uid 1000) y
# necesita permiso de escritura sobre el directorio de plugins montado desde
# el host, que Docker suele crear como root si es la primera vez. Sin esto,
# `confluent-hub install` falla con "componentDir ... not a writeable path"
# y el contenedor queda reiniciándose en bucle sin nunca pasar el healthcheck.
if command -v sudo > /dev/null 2>&1; then
  sudo chown -R 1000:1000 data/connect-plugins 2>/dev/null || \
    chown -R 1000:1000 data/connect-plugins 2>/dev/null || \
    chmod -R 777 data/connect-plugins
else
  chown -R 1000:1000 data/connect-plugins 2>/dev/null || chmod -R 777 data/connect-plugins
fi

echo ">> Construyendo imágenes (Flink personalizado + agentes)..."
docker compose build

echo ">> Levantando el stack..."
docker compose up -d

echo ">> Esperando a que Kafka Connect esté saludable..."
until curl -sf http://localhost:${CONNECT_REST_PORT:-8083}/connectors > /dev/null; do
  sleep 3
done

echo ">> Registrando conectores JDBC (credit_scores, payment_history)..."
curl -s -X POST -H "Content-Type: application/json" \
  --data @connectors/credit-score-source.json \
  http://localhost:${CONNECT_REST_PORT:-8083}/connectors || true

curl -s -X POST -H "Content-Type: application/json" \
  --data @connectors/payment-history-source.json \
  http://localhost:${CONNECT_REST_PORT:-8083}/connectors || true

echo ">> Pre-creando topics de Kafka para evitar carreras con Flink al arrancar..."
for topic in mortgage_applications mortgage_validated_apps mortgage_decisions; do
  docker compose exec -T broker kafka-topics --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1 \
    2>/dev/null || true
done

echo ">> Esperando a que Flink JobManager esté disponible..."
until curl -sf http://localhost:${FLINK_UI_PORT:-8082}/overview > /dev/null; do
  sleep 3
done

echo ">> Desplegando el pipeline de Flink SQL (fuentes + enriquecimiento)..."
docker compose exec -T flink-sql-client bash -c \
  "bin/sql-client.sh -f /opt/flink/sql-scripts/01-pipeline.sql"

echo ">> Job de streaming enviado. Verifícalo en la Flink UI (pestaña Running Jobs)."

cat <<'EOF'

✅ Stack listo.

  Kafbat UI:      http://localhost:8080
  Flink UI:       http://localhost:8082
  Marquez (UI):   http://localhost:3000
  Schema Registry http://localhost:8081
  Connect REST:   http://localhost:8083

Envía una solicitud de prueba con:
  python producer/submit_application.py --name "John Doe" \
    --property-value 200000 --loan-amount 150000 --annual-income 500000

EOF
