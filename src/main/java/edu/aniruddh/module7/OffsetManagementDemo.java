package edu.aniruddh.module7;

import edu.aniruddh.KafkaFundamentalsApp;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * Module 7 — Consumer Offset Management and Deserialization.
 *
 * Demonstrates:
 *   - Auto-commit (at-least-once risk)
 *   - Manual sync commit per batch
 *   - Manual per-record commit (at-least-once with fine granularity)
 *   - seekToBeginning / seek to specific offset (replay)
 *   - Delivery semantics explained
 */
public class OffsetManagementDemo {

    private final String bootstrap;
    private final String topic;

    public OffsetManagementDemo(String bootstrap, String topic) {
        this.bootstrap = bootstrap;
        this.topic = topic;
    }

    public void run() {
        KafkaFundamentalsApp.banner("Module 7: Consumer Offset Management and Deserialization");

        KafkaFundamentalsApp.section("Offset concepts");
        System.out.println("""
                  Offset — the position of the next record to fetch within a partition.

                  __consumer_offsets — internal Kafka topic where committed offsets
                    are stored.  When a consumer restarts it resumes from this value.

                  Delivery semantics:
                    At-most-once  — commit before processing; crash = data loss
                    At-least-once — commit after processing; crash = duplicate delivery
                    Exactly-once  — Kafka Transactions (EOS) across produce + consume""");

        KafkaFundamentalsApp.section("Deserialization");
        System.out.println("""
                  Deserializer<T> is the mirror of Serializer<T>.
                  Type safety tip: if the producer changes its serializer, the consumer
                  will receive garbled data — always version your schemas (Avro/Protobuf).

                  Kafka 4.x note: DeserializationExceptionHandler lets you route
                  poison-pill records to a dead-letter topic instead of crashing.""");

        KafkaFundamentalsApp.section("Manual commit — sync per-batch");
        manualCommitBatch();

        KafkaFundamentalsApp.section("Manual commit — per record with seekToBeginning replay");
        manualCommitPerRecordWithSeek();

        KafkaFundamentalsApp.section("CLI offset management");
        System.out.println("  # Describe offsets for a consumer group");
        System.out.println("  bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \\");
        System.out.println("      --describe --group aniruddh-demo-group");
        System.out.println();
        System.out.println("  # Reset offsets to earliest (requires group to be inactive)");
        System.out.println("  bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \\");
        System.out.println("      --group aniruddh-demo-group --topic kafka-demo \\");
        System.out.println("      --reset-offsets --to-earliest --execute");
    }

    private Properties consumerProps(String groupSuffix, boolean autoCommit) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "aniruddh-offset-" + groupSuffix);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5");
        p.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        p.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000");
        p.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "2000");
        p.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "6000");
        return p;
    }

    private void manualCommitBatch() {
        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(consumerProps("batch", false))) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 6_000;
            int total = 0;

            while (System.currentTimeMillis() < deadline && total < 5) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) continue;

                for (ConsumerRecord<String, String> r : records) {
                    System.out.printf("  Processing offset=%-6d value=%s%n", r.offset(), r.value());
                    total++;
                }
                // commit after entire batch is processed
                consumer.commitSync();
                System.out.println("  commitSync() called — batch committed.");
            }
            if (total == 0) System.out.println("  (no records available — produce some first)");
        } catch (Exception e) {
            System.out.println("  [WARN] " + e.getMessage());
        }
    }

    private void manualCommitPerRecordWithSeek() {
        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(consumerProps("perrecord", false))) {
            consumer.subscribe(List.of(topic));

            // Poll in a short loop until the group coordinator assigns partitions.
            Set<TopicPartition> assigned = Collections.emptySet();
            long assignDeadline = System.currentTimeMillis() + 8_000;
            while (assigned.isEmpty() && System.currentTimeMillis() < assignDeadline) {
                consumer.poll(Duration.ofSeconds(1));
                assigned = consumer.assignment();
            }

            if (assigned.isEmpty()) {
                System.out.println("  (no partitions assigned — is topic empty?)");
                return;
            }

            System.out.println("  Assigned partitions: " + assigned);
            System.out.println("  Seeking to beginning for replay demo...");
            consumer.seekToBeginning(assigned);

            long deadline = System.currentTimeMillis() + 6_000;
            int count = 0;
            Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();

            while (System.currentTimeMillis() < deadline && count < 5) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> r : records) {
                    System.out.printf("  [replay] partition=%d offset=%-6d key=%s%n",
                            r.partition(), r.offset(), r.key());
                    // commit each record individually
                    TopicPartition tp = new TopicPartition(r.topic(), r.partition());
                    toCommit.put(tp, new OffsetAndMetadata(r.offset() + 1));
                    consumer.commitSync(toCommit);
                    toCommit.clear();
                    count++;
                    if (count >= 5) break;
                }
            }
            System.out.println("  Per-record commits done. Count=" + count);
        } catch (Exception e) {
            System.out.println("  [WARN] " + e.getMessage());
        }
    }
}
