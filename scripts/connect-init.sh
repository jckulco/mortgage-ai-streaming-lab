#!/usr/bin/env bash
# Instala el conector JDBC de Confluent Hub si no está ya presente,
# y luego arranca Kafka Connect normalmente.
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

echo ">> Iniciando Kafka Connect..."
exec /etc/confluent/docker/run
