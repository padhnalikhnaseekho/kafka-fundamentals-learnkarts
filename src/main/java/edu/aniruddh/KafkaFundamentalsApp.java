package edu.aniruddh;

import edu.aniruddh.module1.BigDataMessagingDemo;
import edu.aniruddh.module2.KafkaArchitectureDemo;
import edu.aniruddh.module3.ClusterSetupDemo;
import edu.aniruddh.module4.ProducerArchitectureDemo;
import edu.aniruddh.module5.SerializationPartitioningDemo;
import edu.aniruddh.module6.ConsumerArchitectureDemo;
import edu.aniruddh.module7.OffsetManagementDemo;

public class KafkaFundamentalsApp {

    static final String BOOTSTRAP = "localhost:9092";
    static final String DEMO_TOPIC = "kafka-demo";

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
