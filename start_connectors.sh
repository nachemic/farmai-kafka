#!/bin/bash

# Ejecutar desde el directorio del proyecto

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
