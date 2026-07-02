package com.farmia.streaming;

import com.farmia.iot.SensorAlert;
import com.farmia.iot.SensorTelemetry;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class SensorAlerterApp {

    private static final String INPUT_TOPIC = "sensor-telemetry";
    private static final String OUTPUT_TOPIC = "sensor-alerts";
    private static final float TEMP_THRESHOLD = 35.0f;
    private static final float HUMIDITY_THRESHOLD = 20.0f;

    private static Topology createTopology(String schemaRegistryUrl) {
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", schemaRegistryUrl);

        Serde<SensorTelemetry> telemetrySerde = new SpecificAvroSerde<>();
        telemetrySerde.configure(serdeConfig, false);

        Serde<SensorAlert> alertSerde = new SpecificAvroSerde<>();
        alertSerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), telemetrySerde))
                .flatMap((key, telemetry) -> {
                    List<KeyValue<String, SensorAlert>> alerts = new ArrayList<>();
                    String sensorId = telemetry.getSensorId().toString();

                    if (telemetry.getTemperature() > TEMP_THRESHOLD) {
                        alerts.add(KeyValue.pair(sensorId, SensorAlert.newBuilder()
                                .setSensorId(sensorId)
                                .setAlertType("HIGH_TEMPERATURE")
                                .setTimestamp(telemetry.getTimestamp())
                                .setDetails("Temperatura superior a 35°C: " + telemetry.getTemperature())
                                .build()));
                    }

                    if (telemetry.getHumidity() < HUMIDITY_THRESHOLD) {
                        alerts.add(KeyValue.pair(sensorId, SensorAlert.newBuilder()
                                .setSensorId(sensorId)
                                .setAlertType("LOW_HUMIDITY")
                                .setTimestamp(telemetry.getTimestamp())
                                .setDetails("Humedad inferior al 20%: " + telemetry.getHumidity())
                                .build()));
                    }

                    return alerts;
                })
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), alertSerde));

        return builder.build();
    }

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        try (InputStream is = SensorAlerterApp.class.getClassLoader().getResourceAsStream("streams.properties")) {
            props.load(is);
        }
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sensor-alerter-app");

        String schemaRegistryUrl = props.getProperty("schema.registry.url", "http://localhost:8081");
        KafkaStreams streams = new KafkaStreams(createTopology(schemaRegistryUrl), props);
        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));

        try {
            streams.start();
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
