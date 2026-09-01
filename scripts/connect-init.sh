#!/usr/bin/env bash
# Instala el conector JDBC de Confluent Hub si no está ya presente,
# instala el conector Iceberg (compilado con build-iceberg-connector.sh) si
# está disponible, y luego arranca Kafka Connect normalmente.
set -euo pipefail

PLUGIN_DIR="/opt/connect-plugins"
JDBC_DIR="${PLUGIN_DIR}/confluentinc-kafka-connect-jdbc-${JDBC_CONNECTOR_VERSION:-10.8.0}"

if [ ! -d "${JDBC_DIR}" ]; then
  echo ">> Instalando confluentinc/kafka-connect-jdbc:${JDBC_CONNECTOR_VERSION:-10.8.0} ..."
  confluent-hub install --no-prompt --component-dir "${PLUGIN_DIR}" \
    confluentinc/kafka-connect-jdbc:"${JDBC_CONNECTOR_VERSION:-10.8.0}"
else
  echo ">> Conector JDBC ya presente en ${JDBC_DIR}, se omite instalación."
fi

# Conector Iceberg: no se publica precompilado (ver scripts/build-iceberg-connector.sh),
# así que solo lo instalamos si ese script ya dejó un .zip en /opt/connectors-build.
ICEBERG_DIR="${PLUGIN_DIR}/iceberg-kafka-connect-runtime"
ICEBERG_ZIP=$(ls /opt/connectors-build/iceberg-kafka-connect-runtime-*.zip 2>/dev/null | head -n1 || true)

if [ -n "${ICEBERG_ZIP}" ] && [ ! -d "${ICEBERG_DIR}" ]; then
  echo ">> Instalando iceberg-kafka-connect-runtime desde ${ICEBERG_ZIP} ..."
  mkdir -p "${ICEBERG_DIR}"
  unzip -q -o "${ICEBERG_ZIP}" -d "${ICEBERG_DIR}"
elif [ -d "${ICEBERG_DIR}" ]; then
  echo ">> Conector Iceberg ya presente en ${ICEBERG_DIR}, se omite instalación."
else
  echo ">> No se encontró ningún .zip en /opt/connectors-build — el conector"
  echo "   Iceberg no estará disponible hasta correr scripts/build-iceberg-connector.sh"
  echo "   y reiniciar este contenedor. (Los conectores JDBC funcionan igual sin él.)"
fi

echo ">> Iniciando Kafka Connect..."
exec /etc/confluent/docker/run

