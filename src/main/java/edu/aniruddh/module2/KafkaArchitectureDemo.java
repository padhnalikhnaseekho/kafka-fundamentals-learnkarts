package edu.aniruddh.module2;

import edu.aniruddh.KafkaFundamentalsApp;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Module 2 — Introduction to Apache Kafka.
 * Uses AdminClient to inspect live cluster: brokers, topics, partitions, replication.
 */
public class KafkaArchitectureDemo {

    private final String bootstrap;

    public KafkaArchitectureDemo(String bootstrap) {
        this.bootstrap = bootstrap;
    }

    public void run() {
        KafkaFundamentalsApp.banner("Module 2: Introduction to Apache Kafka");

        KafkaFundamentalsApp.section("Core concepts");
        System.out.println("""
                  Broker     — a Kafka server that stores and serves topic data
                  Topic      — a named, ordered, immutable log of records
                  Partition  — a topic is split into N ordered sub-logs for parallelism
                  Replication— each partition has a leader + follower replicas for HA
                  Offset     — monotonically increasing id of each record within a partition
                  KRaft       — Kafka's built-in Raft metadata quorum (replaces ZooKeeper in 4.x)""");

        KafkaFundamentalsApp.section("Live cluster inspection via AdminClient");
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "6000");

        try (AdminClient admin = AdminClient.create(props)) {
            DescribeClusterResult cluster = admin.describeCluster();
            String clusterId = cluster.clusterId().get(6, TimeUnit.SECONDS);
            Node controller = cluster.controller().get(6, TimeUnit.SECONDS);
            Collection<Node> nodes = cluster.nodes().get(6, TimeUnit.SECONDS);

            System.out.println("  Cluster ID  : " + clusterId);
            System.out.println("  Controller  : broker-" + controller.id()
                    + " @ " + controller.host() + ":" + controller.port());
            System.out.println("  Broker count: " + nodes.size());
            nodes.forEach(n -> System.out.printf("    broker-%d  %s:%d (rack=%s)%n",
                    n.id(), n.host(), n.port(), n.rack() == null ? "none" : n.rack()));

            KafkaFundamentalsApp.section("Topics visible on this cluster");
            ListTopicsResult topics = admin.listTopics(new ListTopicsOptions().listInternal(false));
            Set<String> names = topics.names().get(6, TimeUnit.SECONDS);
            if (names.isEmpty()) {
                System.out.println("  (no user topics yet — run Module 3 first)");
            } else {
                Map<String, TopicDescription> desc =
                        admin.describeTopics(names).allTopicNames().get(6, TimeUnit.SECONDS);
                System.out.printf("  %-35s %9s %11s%n", "Topic", "Partitions", "Rep.Factor");
                System.out.printf("  %-35s %9s %11s%n", "-".repeat(35), "-".repeat(9), "-".repeat(11));
                desc.values().stream()
                        .sorted(Comparator.comparing(TopicDescription::name))
                        .forEach(td -> System.out.printf("  %-35s %9d %11d%n",
                                td.name(),
                                td.partitions().size(),
                                td.partitions().get(0).replicas().size()));
            }
        } catch (Exception e) {
            System.out.println("  [WARN] Cannot reach broker at " + bootstrap
                    + " — is Kafka running? (" + e.getMessage() + ")");
            System.out.println("  Tip: cd ~/kafka && bin/kafka-server-start.sh config/kraft/server.properties");
        }
    }
}
