#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════
# Compila el runtime del Apache Iceberg Sink Connector para Kafka Connect.
#
# A diferencia del conector JDBC (que se instala con confluent-hub porque
# Confluent lo publica ya empaquetado), el proyecto Apache Iceberg NO publica
# un .zip/.jar precompilado del "kafka-connect-runtime" en Maven Central ni
# en GitHub Releases — hay que compilarlo desde el código fuente con Gradle.
# Este script lo hace una sola vez, dentro de un contenedor efímero (no
# necesitas Java/Gradle instalado en la VM), y deja el .zip resultante en
# ./connectors/build/, listo para que connect-init.sh lo instale.
#
# Uso:
#   bash scripts/build-iceberg-connector.sh [version_tag]
#   (por defecto compila el tag apache-iceberg-1.9.0)
# ═══════════════════════════════════════════════════════════════════════════
set -euo pipefail
cd "$(dirname "$0")/.."

ICEBERG_TAG="${1:-apache-iceberg-1.9.0}"
OUT_DIR="$(pwd)/connectors/build"
mkdir -p "${OUT_DIR}"

if ls "${OUT_DIR}"/iceberg-kafka-connect-runtime-*.zip > /dev/null 2>&1; then
  echo ">> Ya existe un .zip compilado en ${OUT_DIR}, se omite la compilación."
  echo "   Bórralo manualmente si quieres forzar una recompilación."
  exit 0
fi

echo ">> Compilando iceberg-kafka-connect-runtime (tag ${ICEBERG_TAG})..."
echo "   Esto compila Apache Iceberg desde el código fuente con Gradle dentro"
echo "   de un contenedor efímero. Puede tardar 5-10 minutos la primera vez."

docker run --rm \
  -v "${OUT_DIR}:/out" \
  gradle:8.10-jdk17 \
  bash -c "
    set -e
    git clone --depth 1 --branch '${ICEBERG_TAG}' https://github.com/apache/iceberg.git /tmp/iceberg
    cd /tmp/iceberg
    ./gradlew -x test -x integrationTest :iceberg-kafka-connect:kafka-connect-runtime:build --no-daemon
    cp kafka-connect/kafka-connect-runtime/build/distributions/iceberg-kafka-connect-runtime-*.zip /out/
  "

echo ">> Listo: $(ls "${OUT_DIR}"/iceberg-kafka-connect-runtime-*.zip)"
echo "   Corre 'make start' (o reinicia el contenedor 'connect') para que"
echo "   connect-init.sh lo instale automáticamente en connect-plugins."
