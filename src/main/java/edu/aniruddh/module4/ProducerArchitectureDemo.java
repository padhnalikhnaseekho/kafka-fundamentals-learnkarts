package edu.aniruddh.module4;

import edu.aniruddh.KafkaFundamentalsApp;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Module 4 — Kafka Producer: Architecture and Configuration.
 *
 * Demonstrates:
 *   - Fire-and-forget (acks=0)
 *   - Synchronous send (acks=1, get() on Future)
 *   - Fully reliable async send with callback (acks=all)
 *   - Key producer config knobs explained inline
 */
public class ProducerArchitectureDemo {

    private final String bootstrap;
    private final String topic;

    public ProducerArchitectureDemo(String bootstrap, String topic) {
        this.bootstrap = bootstrap;
        this.topic = topic;
    }

    public void run() throws Exception {
        KafkaFundamentalsApp.banner("Module 4: Kafka Producer — Architecture and Configuration");

        KafkaFundamentalsApp.section("Producer internals");
        System.out.println("""
                  Application → KafkaProducer.send(ProducerRecord)
                      ↓
                  Serializer  — converts key/value to byte[]
                      ↓
                  Partitioner — chooses target partition (key-hash or sticky)
                      ↓
                  RecordAccumulator — batches records per partition
                      ↓
                  Sender thread (background) — flushes batches to broker leader
                      ↓
                  Broker Leader → replicates to followers → returns ACK""");

        KafkaFundamentalsApp.section("Config profiles");
        printConfigComparison();

        try {
            KafkaFundamentalsApp.section("Fire-and-forget (acks=0)");
            fireAndForget();

            KafkaFundamentalsApp.section("Synchronous send (acks=1)");
            synchronousSend();

            KafkaFundamentalsApp.section("Async with callback (acks=all)");
            asyncWithCallback();

        } catch (Exception e) {
            System.out.println("  [WARN] Producer demo skipped — broker unavailable: " + e.getMessage());
        }
    }

    private void printConfigComparison() {
        System.out.printf("  %-25s %-12s %-12s %-16s%n", "Config", "Throughput", "Durability", "Recommended for");
        System.out.printf("  %-25s %-12s %-12s %-16s%n", "-".repeat(25), "-".repeat(12), "-".repeat(12), "-".repeat(16));
        System.out.printf("  %-25s %-12s %-12s %-16s%n", "acks=0 (fire-forget)", "Highest", "None", "Metrics / logs");
        System.out.printf("  %-25s %-12s %-12s %-16s%n", "acks=1 (leader only)", "Medium", "Partial", "General use");
        System.out.printf("  %-25s %-12s %-12s %-16s%n", "acks=all + idempotent", "Lower", "Full", "Financial / events");
        System.out.println();
        System.out.println("  Key tuning knobs:");
        System.out.println("    batch.size      default 16 KB  — larger = higher throughput, more latency");
        System.out.println("    linger.ms       default 0      — adds artificial delay to fill batches");
        System.out.println("    buffer.memory   default 32 MB  — total RecordAccumulator buffer");
        System.out.println("    compression.type lz4/snappy/gzip/zstd — reduces network + disk");
        System.out.println("    enable.idempotence true        — exactly-once within a producer session");
        System.out.println("    max.in.flight.requests.per.connection 5 (idempotent safe in Kafka 4.x)");
    }

    private Properties baseProps() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "6000");
        return p;
    }

    private void fireAndForget() {
        Properties props = baseProps();
        props.put(ProducerConfig.ACKS_CONFIG, "0");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 5; i++) {
                producer.send(new ProducerRecord<>(topic, "key-" + i, "fire-forget-msg-" + i));
            }
            System.out.println("  Sent 5 messages (no ACK wait). Broker may or may not have received them.");
        }
    }

    private void synchronousSend() throws Exception {
        Properties props = baseProps();
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 3; i++) {
                RecordMetadata meta = producer.send(
                        new ProducerRecord<>(topic, "sync-key-" + i, "sync-msg-" + i)).get(6, TimeUnit.SECONDS);
                System.out.printf("  Sent → topic=%s partition=%d offset=%d%n",
                        meta.topic(), meta.partition(), meta.offset());
            }
        }
    }

    private void asyncWithCallback() throws Exception {
        Properties props = baseProps();
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");

        CountDownLatch latch = new CountDownLatch(5);
        AtomicInteger errors = new AtomicInteger(0);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 5; i++) {
                final int seq = i;
                producer.send(
                        new ProducerRecord<>(topic, "cb-key-" + i, "callback-msg-" + i),
                        (meta, ex) -> {
                            if (ex != null) {
                                System.out.println("  [ERROR] seq=" + seq + " : " + ex.getMessage());
                                errors.incrementAndGet();
                            } else {
                                System.out.printf("  ACK  seq=%d → partition=%d offset=%d%n",
                                        seq, meta.partition(), meta.offset());
                            }
                            latch.countDown();
                        });
            }
            latch.await(10, TimeUnit.SECONDS);
            System.out.println("  Done. Errors: " + errors.get());
        }
    }
}
