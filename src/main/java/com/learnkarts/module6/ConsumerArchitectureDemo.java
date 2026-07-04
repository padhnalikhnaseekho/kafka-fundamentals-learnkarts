package com.learnkarts.module6;

import com.learnkarts.KafkaFundamentalsApp;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Module 6 — Kafka Consumer: Architecture and Configuration.
 *
 * Demonstrates:
 *   - Consumer group membership and partition assignment
 *   - ConsumerRebalanceListener (KIP-848 new protocol in Kafka 4.x)
 *   - Listing consumer groups via AdminClient
 *   - Key consumer config parameters
 */
public class ConsumerArchitectureDemo {

    private final String bootstrap;
    private final String topic;
    static final String GROUP_ID = "learnkarts-demo-group";

    public ConsumerArchitectureDemo(String bootstrap, String topic) {
        this.bootstrap = bootstrap;
        this.topic = topic;
    }

    public void run() {
        KafkaFundamentalsApp.banner("Module 6: Kafka Consumer — Architecture and Configuration");

        KafkaFundamentalsApp.section("Consumer internals");
        System.out.println("""
                  Application calls poll(Duration) in a loop — Kafka is PULL-based.
                  Each call:
                    1. Heartbeat sent to Group Coordinator (broker)
                    2. Records fetched from assigned partitions (up to max.poll.records)
                    3. Application processes records
                    4. Offsets committed (auto or manual)

                  Consumer Group:
                    - Multiple consumers sharing a group.id split partition ownership
                    - Each partition is assigned to exactly ONE consumer in the group
                    - Kafka balances: if consumers > partitions, some consumers are idle""");

        KafkaFundamentalsApp.section("Key config parameters");
        System.out.printf("  %-35s %s%n", "Parameter", "Description");
        System.out.printf("  %-35s %s%n", "-".repeat(35), "-".repeat(40));
        System.out.printf("  %-35s %s%n", "group.id",               "Consumer group name (required)");
        System.out.printf("  %-35s %s%n", "auto.offset.reset",      "earliest / latest / none");
        System.out.printf("  %-35s %s%n", "enable.auto.commit",     "true = auto-commit every 5s");
        System.out.printf("  %-35s %s%n", "max.poll.records",       "Max records per poll() call (default 500)");
        System.out.printf("  %-35s %s%n", "fetch.min.bytes",        "Min bytes before broker responds");
        System.out.printf("  %-35s %s%n", "session.timeout.ms",     "Missed heartbeat → rebalance trigger");
        System.out.printf("  %-35s %s%n", "group.protocol (4.x)",   "CLASSIC or CONSUMER (KIP-848 new default)");

        KafkaFundamentalsApp.section("KIP-848 — New Consumer Group Protocol (Kafka 4.x)");
        System.out.println("""
                  Kafka 4.x introduces a server-side rebalancing protocol (KIP-848).
                    group.protocol=CONSUMER  — new incremental cooperative rebalance
                    group.protocol=CLASSIC   — old eager rebalance (stop-the-world)

                  Benefits of the new protocol:
                    - No stop-the-world rebalance; only affected partitions are moved
                    - Reduced latency during scaling events
                    - Managed entirely by the broker's Group Coordinator""");

        KafkaFundamentalsApp.section("Live consumer — read up to 10 messages then exit");
        consumeMessages();

        KafkaFundamentalsApp.section("Consumer groups via AdminClient");
        listConsumerGroups();
    }

    private void consumeMessages() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "2000");
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "6000");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic), new LoggingRebalanceListener());
            int received = 0;
            long deadline = System.currentTimeMillis() + 8_000;

            while (received < 10 && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("  [partition=%d offset=%-6d] key=%-20s value=%s%n",
                            record.partition(), record.offset(), record.key(), record.value());
                    received++;
                    if (received >= 10) break;
                }
            }
            System.out.println("  Total consumed: " + received);
        } catch (Exception e) {
            System.out.println("  [WARN] Consumer failed: " + e.getMessage());
        }
    }

    private void listConsumerGroups() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "6000");

        try (AdminClient admin = AdminClient.create(props)) {
            Collection<ConsumerGroupListing> groups =
                    admin.listConsumerGroups().all().get(6, TimeUnit.SECONDS);
            System.out.printf("  %-35s %s%n", "Group ID", "State");
            System.out.printf("  %-35s %s%n", "-".repeat(35), "-".repeat(15));
            groups.forEach(g -> System.out.printf("  %-35s %s%n",
                    g.groupId(), g.groupState().map(Enum::name).orElse("UNKNOWN")));
        } catch (Exception e) {
            System.out.println("  [WARN] AdminClient error: " + e.getMessage());
        }
    }

    static class LoggingRebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            if (!partitions.isEmpty())
                System.out.println("  [Rebalance] Partitions revoked: " + partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            System.out.println("  [Rebalance] Partitions assigned: " + partitions);
        }
    }
}
