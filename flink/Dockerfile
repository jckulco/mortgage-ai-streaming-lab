# Imagen de Flink con el conector SQL de Kafka pre-instalado.
# Se usa tanto para jobmanager, taskmanager y sql-client (misma imagen,
# distinto comando), igual que recomienda la documentación oficial de Flink.

FROM flink:1.19.1-scala_2.12-java11

ARG FLINK_KAFKA_CONNECTOR_VERSION=3.2.0-1.19

RUN wget -P /opt/flink/lib/ \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${FLINK_KAFKA_CONNECTOR_VERSION}.jar
