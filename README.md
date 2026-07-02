# Kafka y procesamiento de datos en tiempo real

Autor: Ignacio Martínez Godino

Profesor: Jorge Centeno

Máster en Big Data & Data Engineering, Universidad Complutense de Madrid.

Tarea de la asignatura Kafka y procesamiento de datos en tiempo real. El pipeline procesa telemetría de sensores agrícolas y transacciones de ventas sobre Confluent Platform 7.8.0 con Docker Compose.

---

## Arquitectura

```
[Datagen Connector]
       │
       ▼
sensor-telemetry (Avro)
       │
       ▼
[SensorAlerterApp]  ──────────────────────▶  sensor-alerts (Avro)
 Kafka Streams (Java)                                │
 temp > 35°C  →  HIGH_TEMPERATURE                    ▼
 hum  < 20%   →  LOW_HUMIDITY              [MongoDB Sink Connector]
                                                     │
                                                     ▼
                                          MongoDB: course.sensor_alerts


[Datagen Connector]
       │
       ▼
_transactions (Avro)
       │
       ▼
[JDBC Sink Connector]
       │
       ▼
MySQL: sales_transactions
       │
       ▼
[JDBC Source Connector]  ──────▶  sales-transactions (Avro)
 modo timestamp                          │
 key: transaction_id                     ▼
                                  [SalesSummaryApp]
                                  Kafka Streams (Java)
                                         │
                                         ▼
                                  sales-summary (Avro)
```

### Componentes

| Componente | Imagen Docker | Puerto (host) |
|---|---|---|
| Kafka (3 brokers + 3 controladores) | `confluentinc/cp-kafka:7.8.0` | 9092–9094 |
| Schema Registry | `confluentinc/cp-schema-registry:7.8.0` | 8081 |
| Kafka Connect | `confluentinc/cp-kafka-connect:7.8.0` | 8083 |
| Control Center | `confluentinc/cp-enterprise-control-center:7.8.0` | 9021 |
| MySQL | `mysql:8.3` | interno |
| MongoDB | `mongo:8.0` | 27018 |

---

## Prerrequisitos

- Docker Desktop
- Java 17 y Maven 3.x
- Git Bash u otro shell bash compatible

---

## Estructura del proyecto

```
0.tarea/
├── docker-compose.yaml
├── .env
├── pom.xml
├── setup.sh
├── shutdown.sh
├── start_connectors.sh
├── assets/
├── mysql/
│   ├── init.sql
│   └── mysql-connector-java-5.1.45.jar
├── sql/
│   └── ddl.sql
├── connectors/
│   ├── source-datagen-_transactions.json
│   ├── sink-mysql-_transactions.json
│   ├── source-datagen-sensor-telemetry.json   # Tarea 1
│   ├── source-mysql-sales_transactions.json   # Tarea 2
│   └── sink-mongodb-sensor_alerts.json        # Tarea 5
├── datagen/
│   └── _transactions.avsc           
└── src/main/
    ├── avro/
    │   ├── sensor-telemetry.avsc   
    │   ├── sensor-alert.avsc       # Tarea 3
    │   └── sales-summary.avsc      # Tarea 4
    ├── resources/
    │   ├── streams.properties
    │   └── log4j.properties
    └── java/com/farmia/streaming/
        ├── SensorAlerterApp.java   # Tarea 3
        └── SalesSummaryApp.java    # Tarea 4
```

---

## Puesta en marcha

### 1. Levantar el entorno

```bash
./setup.sh
```

Arranca todos los contenedores, crea la tabla en MySQL e instala los plugins de Connect.

> ![Git Bash setup.sh](assets/screenshot1.png)

```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

Deben aparecer 11 contenedores: broker-1/2/3, controller-1/2/3, schema-registry, connect, control-center, mysql, mongodb.

> ![Docker containers status](assets/screenshot2.png)

### 2. Registrar los conectores

Verificar que Connect está listo antes de continuar:

```bash
curl http://localhost:8083/connectors
```

Si devuelve `[]`, ejecutar:

```bash
./start_connectors.sh
```

Comprobar que los conectores están en `RUNNING`:

```bash
curl http://localhost:8083/connectors/source-datagen-sensor-telemetry/status
curl http://localhost:8083/connectors/source-mysql-sales_transactions/status
curl http://localhost:8083/connectors/sink-mongodb-sensor_alerts/status
```

> ![Connectors RUNNING](assets/screenshot3.png)

### 3. Compilar las apps de Kafka Streams

```bash
mvn clean compile
```

El plugin Avro genera las clases Java desde los `.avsc`. Debe terminar en `BUILD SUCCESS`.

### 4. Ejecutar las aplicaciones

Abrir dos terminales en el directorio `0.tarea/`:

**Terminal 1 — Alertas de sensores:**

```bash
mvn exec:java -Dexec.mainClass=com.farmia.streaming.SensorAlerterApp
```

**Terminal 2 — Resúmenes de ventas:**

```bash
mvn exec:java -Dexec.mainClass=com.farmia.streaming.SalesSummaryApp
```

> ![Java Streams apps running](assets/screenshot4.png)

---

## Verificación

### Topics

```bash
docker exec broker-1 kafka-topics --bootstrap-server broker-1:29092 --list
```

Topics relevantes: `sensor-telemetry`, `sensor-alerts`, `sales-transactions`, `sales-summary`.

> ![Topics in Control Center](assets/screenshot5.png)

### Mensajes en tiempo real

Alertas de sensores:

```bash
docker exec schema-registry kafka-avro-console-consumer \
  --bootstrap-server broker-1:29092 \
  --topic sensor-alerts \
  --from-beginning \
  --property schema.registry.url=http://schema-registry:8081
```

Resúmenes de ventas:

```bash
docker exec schema-registry kafka-avro-console-consumer \
  --bootstrap-server broker-1:29092 \
  --topic sales-summary \
  --from-beginning \
  --property schema.registry.url=http://schema-registry:8081
```

Ejemplo de mensaje en `sales-summary`:

```json
{
  "category": "equipment",
  "total_quantity": 73,
  "total_revenue": 7951.27,
  "window_start": 1782846480000,
  "window_end": 1782846540000
}
```

La diferencia `window_end - window_start` es 60.000 ms (1 minuto).

### MongoDB

Contar documentos en la colección de alertas:

```bash
docker exec mongodb mongosh -u admin -p 1234 --authenticationDatabase admin \
  --eval "db.getSiblingDB('course').sensor_alerts.countDocuments()"
```

Ver los últimos documentos:

```bash
docker exec mongodb mongosh -u admin -p 1234 --authenticationDatabase admin \
  --eval "db.getSiblingDB('course').sensor_alerts.find().sort({_id:-1}).limit(5).pretty()"
```

El conteo debe coincidir con el offset del topic `sensor-alerts`.

> ![MongoDB documents count](assets/screenshot6.png)

---

## Conectar MongoDB Compass

URI de conexión:

```
mongodb://admin:1234@localhost:27018/?authSource=admin
```

> ![MongoDB Compass sensor_alerts](assets/screenshot7.png)

---

## Notas de implementación

- **Topics con una partición**: con 3 brokers y el volumen de datos de esta práctica, una sola partición por topic es suficiente. En producción se configuraría el número de particiones según el throughput esperado.
- **Schema compartido para Datagen y Maven**: el fichero `sensor-telemetry.avsc` se usa tanto para el conector Datagen (necesita el schema en disco para generar los datos de prueba) como para el plugin Maven (que genera la clase Java `SensorTelemetry` a partir de él). Los campos `arg.properties` son propios de Datagen y Maven los ignora sin errores.
- **Grace period en la ventana de agregación**: la ventana de 1 minuto de `SalesSummaryApp` incluye un grace period de 1 minuto adicional para tolerar retrasos en la llegada de mensajes desde MySQL.
- **Tipo DECIMAL en Avro**: el campo `price` de MySQL llega serializado como `bytes` en Avro con el driver MySQL 5.1.45. `SalesSummaryApp` incluye una función de conversión para decodificarlo correctamente al calcular `total_revenue`.

---

## Apagar el entorno

```bash
./shutdown.sh
```

Los datos no son persistentes. Al volver a levantar con `./setup.sh` el pipeline empieza desde cero.

> ![Shutdown](assets/screenshot8.png)

---

## Troubleshooting

**MongoDB Compass — "Authentication failed"**
Si tienes MongoDB instalado en Windows puede estar ocupando el puerto 27017 y las conexiones desde Compass no llegan al contenedor. Usar el puerto 27018 en la URI resuelve el problema:
```
mongodb://admin:1234@localhost:27018/?authSource=admin
```

**Kafka Connect tarda en arrancar**
Tiempo de arranque normal: ~2-3 minutos. Si `./start_connectors.sh` da error de conexión, esperar y verificar con `curl http://localhost:8083/connectors` antes de reintentar.
