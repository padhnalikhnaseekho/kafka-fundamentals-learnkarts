package edu.aniruddh.module3;

import edu.aniruddh.KafkaFundamentalsApp;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.TopicConfig;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Module 3 — Kafka Setup and Cluster Configuration (KRaft edition).
 *
 * Kafka 4.x removes ZooKeeper entirely. This module explains the KRaft
 * startup sequence and uses AdminClient to create / describe topics.
 */
public class ClusterSetupDemo {

    private final String bootstrap;
    private final String topic;

    public ClusterSetupDemo(String bootstrap, String topic) {
        this.bootstrap = bootstrap;
        this.topic = topic;
    }

    public void run() {
        KafkaFundamentalsApp.banner("Module 3: Kafka Setup and Cluster Configuration (KRaft)");

        KafkaFundamentalsApp.section("KRaft vs ZooKeeper — what changed in Kafka 4.x");
        System.out.println("""
                  Kafka 3.x  → ZooKeeper optional (KRaft preview)
                  Kafka 4.0  → ZooKeeper REMOVED; KRaft is the ONLY mode

                  KRaft quick-start (single-node, already done if Kafka is running):
                    1. Generate cluster ID
                         bin/kafka-storage.sh random-uuid
                    2. Format storage directory
                         bin/kafka-storage.sh format -t <uuid> -c config/kraft/server.properties
                    3. Start the broker (acts as both broker AND controller)
                         bin/kafka-server-start.sh config/kraft/server.properties

                  Key config in config/kraft/server.properties:
                    process.roles=broker,controller
                    controller.quorum.voters=1@localhost:9093
                    listeners=PLAINTEXT://:9092,CONTROLLER://:9093
                    log.dirs=/tmp/kraft-combined-logs""");

        KafkaFundamentalsApp.section("Topic management via AdminClient");
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "6000");

        try (AdminClient admin = AdminClient.create(props)) {
            ensureTopic(admin, topic, 3, (short) 1);
            describeTopic(admin, topic);
        } catch (Exception e) {
            System.out.println("  [WARN] Broker unreachable: " + e.getMessage());
        }

        KafkaFundamentalsApp.section("Equivalent CLI commands (Kafka 4.x — no --zookeeper flag)");
        System.out.println("  # Create a topic");
        System.out.println("  bin/kafka-topics.sh --bootstrap-server localhost:9092 \\");
        System.out.println("      --create --topic " + topic + " --partitions 3 --replication-factor 1");
        System.out.println();
        System.out.println("  # List topics");
        System.out.println("  bin/kafka-topics.sh --bootstrap-server localhost:9092 --list");
        System.out.println();
        System.out.println("  # Describe topic");
        System.out.println("  bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic " + topic);
        System.out.println();
        System.out.println("  # Inspect KRaft metadata quorum");
        System.out.println("  bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status");
    }

    private void ensureTopic(AdminClient admin, String name, int partitions, short replicas) throws Exception {
        Set<String> existing = admin.listTopics().names().get(6, TimeUnit.SECONDS);
        if (existing.contains(name)) {
            System.out.println("  Topic '" + name + "' already exists — skipping creation.");
            return;
        }
        Map<String, String> topicConfig = Map.of(
                TopicConfig.RETENTION_MS_CONFIG, String.valueOf(TimeUnit.HOURS.toMillis(1)),
                TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE
        );
        NewTopic newTopic = new NewTopic(name, partitions, replicas).configs(topicConfig);
        admin.createTopics(List.of(newTopic)).all().get(6, TimeUnit.SECONDS);
        System.out.println("  Created topic '" + name + "' (" + partitions + " partitions, RF=" + replicas + ")");
    }

    private void describeTopic(AdminClient admin, String name) throws Exception {
        TopicDescription desc = admin.describeTopics(List.of(name))
                .allTopicNames().get(6, TimeUnit.SECONDS).get(name);
        System.out.printf("  Topic : %s  (internal=%b)%n", desc.name(), desc.isInternal());
        desc.partitions().forEach(p ->
                System.out.printf("    partition=%d  leader=broker-%d  replicas=%s  isr=%s%n",
                        p.partition(),
                        p.leader().id(),
                        p.replicas().stream().map(n -> "broker-" + n.id()).toList(),
                        p.isr().stream().map(n -> "broker-" + n.id()).toList()));
    }
}
