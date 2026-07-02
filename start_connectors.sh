#!/bin/bash

echo "Creando topics"
docker exec broker-1 kafka-topics --bootstrap-server broker-1:29092 --create --if-not-exists --topic sensor-telemetry --partitions 1 --replication-factor 3
docker exec broker-1 kafka-topics --bootstrap-server broker-1:29092 --create --if-not-exists --topic sensor-alerts --partitions 1 --replication-factor 3
docker exec broker-1 kafka-topics --bootstrap-server broker-1:29092 --create --if-not-exists --topic sales-transactions --partitions 1 --replication-factor 3
docker exec broker-1 kafka-topics --bootstrap-server broker-1:29092 --create --if-not-exists --topic sales-summary --partitions 1 --replication-factor 3

echo "Lanzando conectores"

curl -s -d @"./connectors/source-datagen-_transactions.json" -H "Content-Type: application/json" -X POST http://localhost:8083/connectors
echo ""

curl -s -d @"./connectors/sink-mysql-_transactions.json" -H "Content-Type: application/json" -X POST http://localhost:8083/connectors
echo ""

curl -s -d @"./connectors/source-datagen-sensor-telemetry.json" -H "Content-Type: application/json" -X POST http://localhost:8083/connectors
echo ""

curl -s -d @"./connectors/source-mysql-sales_transactions.json" -H "Content-Type: application/json" -X POST http://localhost:8083/connectors
echo ""

curl -s -d @"./connectors/sink-mongodb-sensor_alerts.json" -H "Content-Type: application/json" -X POST http://localhost:8083/connectors
echo ""

echo "OK"
