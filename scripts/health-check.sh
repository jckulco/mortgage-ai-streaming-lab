#!/usr/bin/env bash
# Diagnóstico rápido de todos los componentes del stack.
set -uo pipefail
cd "$(dirname "$0")/.."
source .env 2>/dev/null || true

check() {
  local name="$1" url="$2"
  if curl -sf "$url" > /dev/null 2>&1; then
    echo "✅ ${name}"
  else
    echo "❌ ${name} (no responde en ${url})"
  fi
}

echo "── Estado de contenedores ─────────────────────────────"
docker compose ps

echo
echo "── Chequeos HTTP ───────────────────────────────────────"
check "Kafka Connect"     "http://localhost:${CONNECT_REST_PORT:-8083}/connectors"
check "Schema Registry"   "http://localhost:${SCHEMA_REGISTRY_PORT:-8081}/subjects"
check "Flink JobManager"  "http://localhost:${FLINK_UI_PORT:-8082}/overview"
check "Kafbat UI"         "http://localhost:${KAFKA_UI_PORT:-8080}/actuator/health"
check "Marquez API"       "http://localhost:${MARQUEZ_API_PORT:-5000}/api/v1/namespaces"
check "Marquez Web"       "http://localhost:${MARQUEZ_WEB_PORT:-3000}"

echo
echo "── Conectores registrados ──────────────────────────────"
curl -s "http://localhost:${CONNECT_REST_PORT:-8083}/connectors" 2>/dev/null || echo "(sin datos)"
echo
