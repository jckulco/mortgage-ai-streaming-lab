#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════
# Expone el MinIO de watsonx.data developer edition en el HOST de la VM (vía
# kubectl port-forward con auto-reinicio), e imprime las credenciales S3 y
# los valores listos para pegar en .env.
#
# NOTA: a diferencia de un primer intento con esta integración, NO exponemos
# el Hive Metastore de watsonx.data. Investigamos esa ruta (puerto 'thrift'
# 8380 del servicio ibm-lh-mds-thrift-svc) y no expone un protocolo Hive
# Metastore estándar utilizable por clientes externos — solo lo usa
# internamente Presto vía un envoltorio HTTPS+auth propio de IBM (puerto
# 8381). En su lugar, el sink de Kafka Connect usa un catálogo Iceberg tipo
# 'jdbc' respaldado en el Postgres que ya trae este proyecto
# (postgres-mortgage), evitando el HMS por completo. Ver docs/ICEBERG_WATSONX.md.
#
# Detecta el namespace automáticamente (wxd o spark, según la versión del
# instalador — mismo problema documentado en watsonx-data/README.md).
#
# Uso:
#   bash watsonx-data/scripts/get-iceberg-connection-info.sh
#
# El port-forward queda corriendo en background con auto-reinicio (un
# 'kubectl port-forward' normal se cae solo cada cierto tiempo; este script
# lo relanza automáticamente si eso pasa). Para detenerlo:
#   pkill -f 'port-forward-watchdog-minio'
# ═══════════════════════════════════════════════════════════════════════════
set -euo pipefail

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

MINIO_SVC=$(kubectl -n "${NAMESPACE}" get svc -o name | grep -i 'minio' | head -n1 | cut -d/ -f2)

if [ -z "${MINIO_SVC}" ]; then
  echo "!! No se encontró el servicio de MinIO en el namespace ${NAMESPACE}."
  echo "   Revisa manualmente con: kubectl -n ${NAMESPACE} get svc"
  exit 1
fi

echo ">> Servicio MinIO: ${MINIO_SVC}"

# Mata cualquier watchdog previo para no duplicar procesos si se re-ejecuta.
pkill -f 'port-forward-watchdog-minio' 2>/dev/null || true
sleep 1

echo ">> Exponiendo MinIO S3 en 0.0.0.0:${S3_LOCAL_PORT} (con auto-reinicio)..."
# Un 'kubectl port-forward' normal se cae solo cada cierto tiempo (timeout de
# inactividad, reinicio del pod, etc.) — este bucle lo relanza sin que tengas
# que hacerlo a mano cada vez que un conector empieza a fallar con
# "Connection refused".
nohup bash -c "
  export PS1='port-forward-watchdog-minio'
  while true; do
    kubectl -n '${NAMESPACE}' port-forward --address 0.0.0.0 'svc/${MINIO_SVC}' '${S3_LOCAL_PORT}:9000'
    echo \"[\$(date)] port-forward de MinIO se cayó, reintentando en 2s...\" 
    sleep 2
  done
" > /tmp/wxd-minio-port-forward.log 2>&1 &
disown

sleep 3
if timeout 3 bash -c "cat < /dev/null > /dev/tcp/localhost/${S3_LOCAL_PORT}" 2>/dev/null; then
  echo ">> ✅ Puerto ${S3_LOCAL_PORT} respondiendo."
else
  echo ">> ⚠️  El puerto ${S3_LOCAL_PORT} no respondió en 3s — revisa /tmp/wxd-minio-port-forward.log"
fi

# Las credenciales S3 de watsonx.data se guardan como variables de entorno
# LH_S3_ACCESS_KEY / LH_S3_SECRET_KEY dentro de los pods del lakehouse (en
# la instalación developer edition observada, ambas son literalmente
# "dummyvalue" — pero las leemos igual por si tu instalación difiere).
ACCESS_KEY=$(kubectl -n "${NAMESPACE}" exec deploy/ibm-lh-minio -- \
  printenv LH_S3_ACCESS_KEY 2>/dev/null || true)
SECRET_KEY=$(kubectl -n "${NAMESPACE}" exec deploy/ibm-lh-minio -- \
  printenv LH_S3_SECRET_KEY 2>/dev/null || true)

if [ -z "${ACCESS_KEY}" ] || [ -z "${SECRET_KEY}" ]; then
  echo ""
  echo "!! No se pudieron leer LH_S3_ACCESS_KEY / LH_S3_SECRET_KEY automáticamente."
  echo "   Búscalas manualmente, por ejemplo:"
  echo "     kubectl -n ${NAMESPACE} get pods   # identifica el pod de minio"
  echo "     kubectl -n ${NAMESPACE} exec <pod> -- printenv | grep LH_S3"
  echo "   (En muchas instalaciones developer edition ambas son 'dummyvalue'.)"
fi

cat <<EOF

✅ Listo. Pega esto en tu .env (junto con ICEBERG_ENABLED=true):

ICEBERG_S3_HOST=host.docker.internal
ICEBERG_S3_PORT=${S3_LOCAL_PORT}
ICEBERG_S3_ACCESS_KEY=${ACCESS_KEY:-dummyvalue}
ICEBERG_S3_SECRET_KEY=${SECRET_KEY:-dummyvalue}

Recuerda además crear el bucket (ICEBERG_BUCKET, por defecto 'iceberg-bucket'
— reutiliza el que watsonx.data ya trae por defecto en desarrollo) si aún no
existe, desde la consola MinIO de watsonx.data.

El port-forward quedó corriendo en background CON auto-reinicio. Para
detenerlo por completo:
  pkill -f 'port-forward-watchdog-minio'
  pkill -f 'port-forward.*${MINIO_SVC}'
EOF
