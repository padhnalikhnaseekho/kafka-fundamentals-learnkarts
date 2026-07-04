package edu.aniruddh.module5;

import edu.aniruddh.KafkaFundamentalsApp;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Module 5 — Producer Serialization and Partitioning.
 *
 * Demonstrates:
 *   - Custom Serializer (OrderEvent POJO → bytes)
 *   - Custom Partitioner (route by region prefix in the key)
 *   - Key-based routing (same key → same partition)
 */
public class SerializationPartitioningDemo {

    private final String bootstrap;
    private final String topic;

    public SerializationPartitioningDemo(String bootstrap, String topic) {
        this.bootstrap = bootstrap;
        this.topic = topic;
    }

    public void run() throws Exception {
        KafkaFundamentalsApp.banner("Module 5: Serialization and Partitioning");

        KafkaFundamentalsApp.section("Serialization explained");
        System.out.println("""
                  Serializer<T> converts a Java object → byte[] before sending.
                  Deserializer<T> reverses it on the consumer side.

                  Built-in serializers (kafka-clients):
                    StringSerializer / StringDeserializer
                    IntegerSerializer / IntegerDeserializer
                    BytesSerializer  / BytesDeserializer
                    LongSerializer   / LongDeserializer

                  Schema-based (external libraries):
                    Avro    — org.apache.kafka:kafka-avro-serializer (Confluent)
                    Protobuf— io.confluent:kafka-protobuf-serializer
                    JSON    — typically Jackson-backed custom impl""");

        KafkaFundamentalsApp.section("Custom OrderEvent serializer demo");
        demonstrateCustomSerializer();

        KafkaFundamentalsApp.section("Partitioning strategies");
        System.out.println("""
                  Default (Kafka 4.x) — sticky partitioner: fills one batch fully
                    before switching partition. Better throughput than pure round-robin.
                  Key-based           — murmur2(key) % numPartitions; same key always
                    goes to the same partition, guaranteeing order per key.
                  Custom partitioner  — implement Partitioner interface.""");

        KafkaFundamentalsApp.section("Custom RegionPartitioner demo");
        demonstrateCustomPartitioner();
    }

    // ------------------------------------------------------------------ demo 1
    private void demonstrateCustomSerializer() {
        OrderEventSerializer ser = new OrderEventSerializer();
        OrderEvent event = new OrderEvent("ORD-001", "APAC", 299.99);
        byte[] bytes = ser.serialize(topic, event);
        System.out.println("  OrderEvent : " + event);
        System.out.println("  Serialized : " + new String(bytes, StandardCharsets.UTF_8));

        // Deserialise back to verify round-trip
        OrderEventDeserializer deser = new OrderEventDeserializer();
        OrderEvent roundTripped = deser.deserialize(topic, bytes);
        System.out.println("  Round-trip : " + roundTripped);
    }

    // ------------------------------------------------------------------ demo 2
    private void demonstrateCustomPartitioner() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, RegionPartitioner.class.getName());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "6000");

        String[] regions = {"APAC", "EMEA", "AMER"};

        System.out.println("  Sending messages with region-prefix keys (RegionPartitioner):");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (String region : regions) {
                for (int i = 0; i < 2; i++) {
                    String key = region + ":order-" + i;
                    String value = "{\"region\":\"" + region + "\",\"seq\":" + i + "}";
                    producer.send(new ProducerRecord<>(topic, key, value), (meta, ex) -> {
                        if (ex == null) {
                            System.out.printf("    key=%-20s → partition=%d offset=%d%n",
                                    key, meta.partition(), meta.offset());
                        } else {
                            System.out.println("    [WARN] " + ex.getMessage());
                        }
                    });
                }
            }
        } catch (Exception e) {
            System.out.println("  [WARN] Broker unreachable: " + e.getMessage());
        }
    }

    // ================================================================== types

    /** Simple POJO representing an order event. */
    public record OrderEvent(String orderId, String region, double amount) {
        @Override public String toString() {
            return "OrderEvent{orderId='" + orderId + "', region='" + region + "', amount=" + amount + "}";
        }
    }

    /** Serializes OrderEvent to a simple CSV-like byte string. */
    public static class OrderEventSerializer implements Serializer<OrderEvent> {
        @Override
        public byte[] serialize(String topic, OrderEvent data) {
            if (data == null) return null;
            String csv = data.orderId() + "," + data.region() + "," + data.amount();
            return csv.getBytes(StandardCharsets.UTF_8);
        }
    }

    public static class OrderEventDeserializer implements org.apache.kafka.common.serialization.Deserializer<OrderEvent> {
        @Override
        public OrderEvent deserialize(String topic, byte[] data) {
            if (data == null) return null;
            String[] parts = new String(data, StandardCharsets.UTF_8).split(",");
            return new OrderEvent(parts[0], parts[1], Double.parseDouble(parts[2]));
        }
    }

    /**
     * Routes messages to partitions based on a region prefix in the key.
     * APAC → partition 0, EMEA → partition 1, AMER → partition 2, rest → hash.
     */
    public static class RegionPartitioner implements Partitioner {
        private static final Map<String, Integer> REGION_MAP =
                Map.of("APAC", 0, "EMEA", 1, "AMER", 2);

        @Override
        public int partition(String topic, Object key, byte[] keyBytes,
                             Object value, byte[] valueBytes, Cluster cluster) {
            int numPartitions = cluster.partitionsForTopic(topic).size();
            if (keyBytes == null || numPartitions == 0) return 0;
            String keyStr = new String(keyBytes, StandardCharsets.UTF_8);
            String prefix = keyStr.contains(":") ? keyStr.split(":")[0] : "";
            if (REGION_MAP.containsKey(prefix)) {
                return REGION_MAP.get(prefix) % numPartitions;
            }
            return Math.abs(Arrays.hashCode(keyBytes)) % numPartitions;
        }

        @Override public void close() {}
        @Override public void configure(Map<String, ?> configs) {}
    }
}
