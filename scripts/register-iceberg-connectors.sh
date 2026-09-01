#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════
# Registra (o actualiza) los dos conectores sink que escriben en Iceberg,
# sobre el bucket MinIO de watsonx.data developer edition.
#
# Requiere:
#   1. ICEBERG_ENABLED=true y las demás variables ICEBERG_* en .env
#   2. El .zip del conector compilado (scripts/build-iceberg-connector.sh)
#      ya instalado — reinicia el contenedor 'connect' después de compilarlo.
#   3. MinIO de watsonx.data expuesto en el host (ver
#      docs/ICEBERG_WATSONX.md), y el bucket ICEBERG_BUCKET ya creado.
#
# Uso:
#   bash scripts/register-iceberg-connectors.sh
# ═══════════════════════════════════════════════════════════════════════════
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -f .env ]; then
  set -a; source .env; set +a
fi

: "${ICEBERG_NAMESPACE:?Falta ICEBERG_NAMESPACE en .env}"
: "${ICEBERG_S3_HOST:?Falta ICEBERG_S3_HOST en .env}"
: "${ICEBERG_S3_PORT:?Falta ICEBERG_S3_PORT en .env}"
: "${ICEBERG_BUCKET:?Falta ICEBERG_BUCKET en .env}"

if [ -z "${ICEBERG_S3_ACCESS_KEY:-}" ] || [ -z "${ICEBERG_S3_SECRET_KEY:-}" ]; then
  echo "!! ICEBERG_S3_ACCESS_KEY / ICEBERG_S3_SECRET_KEY vacíos en .env."
  echo "   Consíguelos con: bash watsonx-data/scripts/get-iceberg-connection-info.sh"
  exit 1
fi

CONNECT_URL="http://localhost:${CONNECT_REST_PORT:-8083}"

for template in connectors/iceberg-enriched-sink.json.template connectors/iceberg-decisions-sink.json.template; do
  rendered="/tmp/$(basename "${template%.template}")"
  envsubst < "${template}" > "${rendered}"
  name=$(python3 -c "import json;print(json.load(open('${rendered}'))['name'])")

  echo ">> Registrando/actualizando conector '${name}'..."
  # PUT sobre /connectors/<name>/config crea o actualiza sin necesidad de
  # borrar primero (a diferencia de POST /connectors, que falla si ya existe).
  config_only=$(python3 -c "import json;print(json.dumps(json.load(open('${rendered}'))['config']))")
  curl -s -X PUT -H "Content-Type: application/json" \
    --data "${config_only}" \
    "${CONNECT_URL}/connectors/${name}/config" | python3 -m json.tool || true
done

echo ">> Listo. Verifica el estado con:"
echo "   curl -s ${CONNECT_URL}/connectors/iceberg-enriched-sink/status | python3 -m json.tool"
echo "   curl -s ${CONNECT_URL}/connectors/iceberg-decisions-sink/status | python3 -m json.tool"
