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
# El build genera DOS distribuciones: la estándar y una "-hive-" que incluye el
# cliente de Hive Metastore. Usamos la variante "-hive-" (trae más dependencias
# empaquetadas, incluye clases que el catálogo JDBC también necesita).
ICEBERG_DIR="${PLUGIN_DIR}/iceberg-kafka-connect-runtime"
# Ojo: una carpeta vacía (de un intento fallido anterior) no cuenta como instalada.
ICEBERG_ALREADY_INSTALLED="false"
if [ -d "${ICEBERG_DIR}" ] && [ -n "$(ls -A "${ICEBERG_DIR}" 2>/dev/null)" ]; then
  ICEBERG_ALREADY_INSTALLED="true"
fi
ICEBERG_ZIP=$(ls /opt/connectors-build/iceberg-kafka-connect-runtime-hive-*.zip 2>/dev/null | head -n1 || true)
if [ -z "${ICEBERG_ZIP}" ]; then
  # Fallback por si algún día solo existe la variante estándar (sin soporte Hive).
  ICEBERG_ZIP=$(ls /opt/connectors-build/iceberg-kafka-connect-runtime-*.zip 2>/dev/null | head -n1 || true)
fi

if [ -n "${ICEBERG_ZIP}" ] && [ "${ICEBERG_ALREADY_INSTALLED}" = "false" ]; then
  echo ">> Instalando iceberg-kafka-connect-runtime desde ${ICEBERG_ZIP} ..."
  rm -rf "${ICEBERG_DIR}"
  mkdir -p "${ICEBERG_DIR}"
  # La imagen no trae 'unzip'; 'jar' sí viene con el JDK y sabe extraer .zip.
  # 'jar xf' extrae en el directorio ACTUAL, así que hay que moverse primero.
  (cd "${ICEBERG_DIR}" && jar xf "${ICEBERG_ZIP}")
  # El .zip trae todo dentro de una carpeta raíz tipo
  # iceberg-kafka-connect-runtime-hive-1.10.0-SNAPSHOT/ — la aplanamos para
  # que los .jar queden directo en ICEBERG_DIR (formato que espera Connect).
  inner_dir=$(find "${ICEBERG_DIR}" -mindepth 1 -maxdepth 1 -type d | head -n1)
  if [ -n "${inner_dir}" ]; then
    mv "${inner_dir}"/* "${ICEBERG_DIR}"/
    rmdir "${inner_dir}"
  fi
elif [ "${ICEBERG_ALREADY_INSTALLED}" = "true" ]; then
  echo ">> Conector Iceberg ya presente en ${ICEBERG_DIR}, se omite instalación."
else
  echo ">> No se encontró ningún .zip en /opt/connectors-build — el conector"
  echo "   Iceberg no estará disponible hasta correr scripts/build-iceberg-connector.sh"
  echo "   y reiniciar este contenedor. (Los conectores JDBC funcionan igual sin él.)"
fi

# El catálogo Iceberg tipo 'jdbc' (usado para escribir en el bucket de
# watsonx.data sin depender del Hive Metastore de IBM) necesita el driver de
# PostgreSQL en el MISMO classloader del plugin Iceberg. El conector JDBC de
# Confluent ya lo trae — lo reutilizamos en vez de descargarlo aparte.
if [ -d "${ICEBERG_DIR}" ]; then
  PG_DRIVER=$(find "${PLUGIN_DIR}" -maxdepth 3 -iname "postgresql-*.jar" ! -path "${ICEBERG_DIR}/*" | head -n1 || true)
  if [ -n "${PG_DRIVER}" ] && [ ! -f "${ICEBERG_DIR}/$(basename "${PG_DRIVER}")" ]; then
    echo ">> Copiando driver PostgreSQL ($(basename "${PG_DRIVER}")) al plugin Iceberg..."
    cp "${PG_DRIVER}" "${ICEBERG_DIR}/"
  fi
fi

echo ">> Iniciando Kafka Connect..."
exec /etc/confluent/docker/run

