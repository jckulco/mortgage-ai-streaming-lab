#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════
# Expone el Hive Metastore (HMS) y el MinIO S3 de watsonx.data developer
# edition en el HOST de la VM (vía kubectl port-forward), e imprime las
# credenciales S3 y los valores listos para pegar en .env, para que Kafka
# Connect (que corre en Docker, fuera del cluster KIND) pueda escribir
# Iceberg tables directamente en el mismo storage que usa watsonx.data.
#
# Detecta el namespace automáticamente (wxd o spark, según la versión del
# instalador — mismo problema documentado en watsonx-data/README.md).
#
# Uso:
#   bash watsonx-data/scripts/get-iceberg-connection-info.sh
#
# Deja los port-forwards corriendo en background (nohup); para detenerlos:
#   pkill -f 'port-forward.*ibm-lh-mds-thrift'
#   pkill -f 'port-forward.*ibm-lh-minio'
# ═══════════════════════════════════════════════════════════════════════════
set -euo pipefail

HMS_LOCAL_PORT="${HMS_LOCAL_PORT:-9083}"
S3_LOCAL_PORT="${S3_LOCAL_PORT:-9000}"

NAMESPACE=""
for ns in wxd spark; do
  if kubectl get ns "${ns}" > /dev/null 2>&1; then
    NAMESPACE="${ns}"
    break
  fi
done

if [ -z "${NAMESPACE}" ]; then
  echo "!! No se encontró el namespace 'wxd' ni 'spark'. ¿Está instalado watsonx.data"
  echo "   developer edition y corriendo (docker ps | grep kind-wxd-control-plane)?"
  exit 1
fi

echo ">> Namespace detectado: ${NAMESPACE}"

HMS_SVC=$(kubectl -n "${NAMESPACE}" get svc -o name | grep -i 'mds-thrift' | head -n1 | cut -d/ -f2)
MINIO_SVC=$(kubectl -n "${NAMESPACE}" get svc -o name | grep -i 'minio' | head -n1 | cut -d/ -f2)

if [ -z "${HMS_SVC}" ] || [ -z "${MINIO_SVC}" ]; then
  echo "!! No se encontraron los servicios de HMS o MinIO en el namespace ${NAMESPACE}."
  echo "   Revisa manualmente con: kubectl -n ${NAMESPACE} get svc"
  exit 1
fi

echo ">> Servicio HMS:   ${HMS_SVC}"
echo ">> Servicio MinIO: ${MINIO_SVC}"

echo ">> Exponiendo HMS en 0.0.0.0:${HMS_LOCAL_PORT} (background)..."
nohup kubectl -n "${NAMESPACE}" port-forward --address 0.0.0.0 \
  "svc/${HMS_SVC}" "${HMS_LOCAL_PORT}:9083" \
  > /tmp/wxd-hms-port-forward.log 2>&1 &
disown

echo ">> Exponiendo MinIO S3 en 0.0.0.0:${S3_LOCAL_PORT} (background)..."
nohup kubectl -n "${NAMESPACE}" port-forward --address 0.0.0.0 \
  "svc/${MINIO_SVC}" "${S3_LOCAL_PORT}:9000" \
  > /tmp/wxd-minio-port-forward.log 2>&1 &
disown

sleep 3

# Las credenciales S3 de watsonx.data se guardan como variables de entorno
# LH_S3_ACCESS_KEY / LH_S3_SECRET_KEY dentro de los pods del lakehouse.
ACCESS_KEY=$(kubectl -n "${NAMESPACE}" exec deploy/ibm-lh-presto-svc -- \
  printenv LH_S3_ACCESS_KEY 2>/dev/null || true)
SECRET_KEY=$(kubectl -n "${NAMESPACE}" exec deploy/ibm-lh-presto-svc -- \
  printenv LH_S3_SECRET_KEY 2>/dev/null || true)

if [ -z "${ACCESS_KEY}" ] || [ -z "${SECRET_KEY}" ]; then
  echo ""
  echo "!! No se pudieron leer LH_S3_ACCESS_KEY / LH_S3_SECRET_KEY automáticamente."
  echo "   Búscalas manualmente, por ejemplo:"
  echo "     kubectl -n ${NAMESPACE} get pods   # identifica el pod de presto o minio"
  echo "     kubectl -n ${NAMESPACE} exec <pod> -- printenv | grep LH_S3"
  echo "   O en la consola de watsonx.data: Infrastructure manager > tu bucket > credenciales."
fi

cat <<EOF

✅ Listo. Pega esto en tu .env (junto con ICEBERG_ENABLED=true):

ICEBERG_HMS_HOST=host.docker.internal
ICEBERG_HMS_PORT=${HMS_LOCAL_PORT}
ICEBERG_S3_HOST=host.docker.internal
ICEBERG_S3_PORT=${S3_LOCAL_PORT}
ICEBERG_S3_ACCESS_KEY=${ACCESS_KEY:-<pégala_manualmente>}
ICEBERG_S3_SECRET_KEY=${SECRET_KEY:-<pégala_manualmente>}

Recuerda además crear el bucket (ICEBERG_BUCKET, por defecto 'mortgage-lab')
en la consola MinIO de watsonx.data antes de registrar los conectores.

Los port-forwards quedaron corriendo en background. Para detenerlos:
  pkill -f 'port-forward.*${HMS_SVC}'
  pkill -f 'port-forward.*${MINIO_SVC}'
EOF
