package com.farmia.streaming;

import com.farmia.sales.SalesSummary;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class SalesSummaryApp {

    private static final String INPUT_TOPIC = "sales-transactions";
    private static final String OUTPUT_TOPIC = "sales-summary";

    // El driver MySQL 5.1.45 serializa DECIMAL(10,2) como bytes con logicalType:decimal en lugar de double
    private static double extractDouble(GenericRecord tx, String fieldName) {
        Object value = tx.get(fieldName);
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        if (value instanceof ByteBuffer) {
            Schema fieldSchema = tx.getSchema().getField(fieldName).schema();
            LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) fieldSchema.getLogicalType();
            BigDecimal decimal = new Conversions.DecimalConversion()
                    .fromBytes((ByteBuffer) value, fieldSchema, decimalType);
            return decimal.doubleValue();
        }
        throw new IllegalArgumentException("Unsupported numeric type for field '" + fieldName
                + "': " + value.getClass());
    }

    private static Topology createTopology(String schemaRegistryUrl) {
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", schemaRegistryUrl);

        Serde<GenericRecord> genericSerde = new GenericAvroSerde();
        genericSerde.configure(serdeConfig, false);

        Serde<SalesSummary> summarySerde = new SpecificAvroSerde<>();
        summarySerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), genericSerde))
                .selectKey((key, tx) -> tx.get("category").toString())
                .groupByKey(Grouped.with(Serdes.String(), genericSerde))
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1), Duration.ofMinutes(1)))
                .aggregate(
                        () -> SalesSummary.newBuilder()
                                .setCategory("")
                                .setTotalQuantity(0L)
                                .setTotalRevenue(0.0)
                                .setWindowStart(0L)
                                .setWindowEnd(0L)
                                .build(),
                        (category, tx, acc) -> {
                            int quantity = (Integer) tx.get("quantity");
                            double price = extractDouble(tx, "price");
                            return SalesSummary.newBuilder()
                                    .setCategory(category)
                                    .setTotalQuantity(acc.getTotalQuantity() + quantity)
                                    .setTotalRevenue(acc.getTotalRevenue() + (quantity * price))
                                    .setWindowStart(0L)
                                    .setWindowEnd(0L)
                                    .build();
                        },
                        Materialized.with(Serdes.String(), summarySerde)
                )
                .toStream()
                .map((windowedKey, summary) -> KeyValue.pair(
                        windowedKey.key(),
                        SalesSummary.newBuilder()
                                .setCategory(summary.getCategory().toString())
                                .setTotalQuantity(summary.getTotalQuantity())
                                .setTotalRevenue(summary.getTotalRevenue())
                                .setWindowStart(windowedKey.window().start())
                                .setWindowEnd(windowedKey.window().end())
                                .build()
                ))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), summarySerde));

        return builder.build();
    }

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        try (InputStream is = SalesSummaryApp.class.getClassLoader().getResourceAsStream("streams.properties")) {
            props.load(is);
        }
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sales-summary-app");

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
