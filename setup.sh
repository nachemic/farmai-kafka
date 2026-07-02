#!/bin/bash

echo "Iniciando entorno"
docker compose up -d
sleep 30

echo "Creando la tabla transactions"
docker cp ./sql/ddl.sql mysql:/
docker exec mysql bash -c "mysql --user=root --password=1234 --database=db < /ddl.sql"

echo "Instalando conectores..."
docker compose exec connect confluent-hub install --no-prompt confluentinc/kafka-connect-datagen:latest
docker compose exec connect confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:latest
docker compose exec connect confluent-hub install --no-prompt mongodb/kafka-connect-mongodb:latest
docker compose exec connect confluent-hub install --no-prompt jcustenborder/kafka-connect-transform-common:latest

# Kafka Connect JDBC no tiene el driver de MySQL y se tiene que instalar manualmente en el directorio del plugin

echo "Copiando driver MySQL..."
docker cp ./mysql/mysql-connector-java-5.1.45.jar connect:/usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/mysql-connector-java-5.1.45.jar

echo "Copiando schemas AVRO..."
docker cp ./src/main/avro/sensor-telemetry.avsc connect:/home/appuser/
docker cp ./datagen/_transactions.avsc connect:/home/appuser/

echo "Reiniciando contenedor connect..."
docker compose restart connect
echo "Esperando reinicio contenedor connect"
sleep 30

echo "OK"
