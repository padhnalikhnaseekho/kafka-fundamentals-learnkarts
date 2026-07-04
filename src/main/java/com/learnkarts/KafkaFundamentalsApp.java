package com.learnkarts;

import com.learnkarts.module1.BigDataMessagingDemo;
import com.learnkarts.module2.KafkaArchitectureDemo;
import com.learnkarts.module3.ClusterSetupDemo;
import com.learnkarts.module4.ProducerArchitectureDemo;
import com.learnkarts.module5.SerializationPartitioningDemo;
import com.learnkarts.module6.ConsumerArchitectureDemo;
import com.learnkarts.module7.OffsetManagementDemo;

public class KafkaFundamentalsApp {

    static final String BOOTSTRAP = "localhost:9092";
    static final String DEMO_TOPIC = "learnkarts-demo";

    public static void main(String[] args) throws Exception {
        banner("Apache Kafka 4.x Fundamentals — LearnKarts Demo");
        System.out.println("  Kafka bootstrap : " + BOOTSTRAP);
        System.out.println("  Demo topic      : " + DEMO_TOPIC);
        System.out.println();

        new BigDataMessagingDemo().run();
        new KafkaArchitectureDemo(BOOTSTRAP).run();
        new ClusterSetupDemo(BOOTSTRAP, DEMO_TOPIC).run();
        new ProducerArchitectureDemo(BOOTSTRAP, DEMO_TOPIC).run();
        new SerializationPartitioningDemo(BOOTSTRAP, DEMO_TOPIC).run();
        new ConsumerArchitectureDemo(BOOTSTRAP, DEMO_TOPIC).run();
        new OffsetManagementDemo(BOOTSTRAP, DEMO_TOPIC).run();

        banner("All modules complete");
    }

    public static void banner(String title) {
        String bar = "=".repeat(60);
        System.out.println("\n" + bar);
        System.out.println("  " + title);
        System.out.println(bar);
    }

    public static void section(String heading) {
        System.out.println("\n--- " + heading + " ---");
    }
}
