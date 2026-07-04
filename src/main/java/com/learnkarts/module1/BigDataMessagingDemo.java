package com.learnkarts.module1;

import com.learnkarts.KafkaFundamentalsApp;

/**
 * Module 1 — Big Data and Messaging Fundamentals.
 * No Kafka connection required; illustrates concepts through structured output.
 */
public class BigDataMessagingDemo {

    public void run() {
        KafkaFundamentalsApp.banner("Module 1: Big Data and Messaging Fundamentals");

        KafkaFundamentalsApp.section("Why real-time processing?");
        System.out.println("""
                Modern data systems face three pressures:
                  Volume   — terabytes generated every minute (IoT, social, transactions)
                  Velocity — decisions needed in milliseconds, not hours
                  Variety  — JSON, Avro, Protobuf, binary blobs all co-exist

                Batch ETL cannot satisfy sub-second SLAs. Event streaming bridges
                the gap between raw data production and real-time insight.""");

        KafkaFundamentalsApp.section("Messaging patterns");
        System.out.printf("  %-20s %s%n", "Pattern", "Characteristic");
        System.out.printf("  %-20s %s%n", "-".repeat(20), "-".repeat(35));
        System.out.printf("  %-20s %s%n", "Point-to-Point",  "One producer → one consumer, message deleted after ACK");
        System.out.printf("  %-20s %s%n", "Publish-Subscribe","One producer → many consumers, durable log retained");
        System.out.printf("  %-20s %s%n", "Request-Reply",   "Synchronous RPC over a message bus");

        KafkaFundamentalsApp.section("Traditional systems and their limits");
        System.out.println("""
                  RabbitMQ / ActiveMQ  → push-based, messages gone after consumption
                  RDBMS queues         → poor throughput, high lock contention
                  Apache Kafka         → pull-based, immutable log, horizontal scale

                Kafka's commit-log design means any consumer can replay history,
                enabling multiple independent downstream applications from one stream.""");
    }
}
