# Kafka Fundamentals — LearnKarts Demo App

Java 21 + Gradle application that walks through every module of the *Apache Kafka Fundamentals* Coursera syllabus using **Kafka 4.3.1** (KRaft, no ZooKeeper).

---

## Prerequisites

| Tool | Version used |
|------|-------------|
| Java | 21 (OpenJDK) |
| Gradle | 8.11.1 (via wrapper) |
| Apache Kafka | **4.3.1** (KRaft mode) |

---

## Project layout

```
coursera-learnkarts/
├── build.gradle
├── settings.gradle
├── gradlew / gradlew.bat
├── kafka_fundamentals_syllabus.md   ← annotated for Kafka 4.x
├── README.md                        ← this file
└── src/main/java/com/learnkarts/
    ├── KafkaFundamentalsApp.java    ← main entry point
    ├── module1/BigDataMessagingDemo.java
    ├── module2/KafkaArchitectureDemo.java
    ├── module3/ClusterSetupDemo.java
    ├── module4/ProducerArchitectureDemo.java
    ├── module5/SerializationPartitioningDemo.java
    ├── module6/ConsumerArchitectureDemo.java
    └── module7/OffsetManagementDemo.java
```

---

## Start Kafka (KRaft, single-node)

```bash
# 1. Generate a cluster UUID (only once per storage dir)
CLUSTER_ID=$(~/kafka/bin/kafka-storage.sh random-uuid)

# 2. Format storage
~/kafka/bin/kafka-storage.sh format \
    -t "$CLUSTER_ID" \
    -c ~/kafka/config/kraft/server.properties

# 3. Start the broker
~/kafka/bin/kafka-server-start.sh ~/kafka/config/kraft/server.properties
```

> **Kafka 4.x**: no `--zookeeper` flag anywhere. ZooKeeper was fully removed in 4.0.

---

## Build and run

```bash
cd /home/anrd/git/kafka/coursera-learnkarts

# First time — generate Gradle wrapper using downloaded distribution
~/Downloads/gradle-8.11.1/bin/gradle wrapper --gradle-version 8.11.1

# Build fat jar
./gradlew jar

# Run (Kafka must be running)
./gradlew run

# Or run the fat jar directly
java -jar build/libs/kafka-fundamentals-1.0.0.jar
```

---

## Module breakdown

### Module 1 — Big Data and Messaging Fundamentals
*No Kafka connection required.*  
Prints a structured overview of modern data pressures (Volume/Velocity/Variety), compares messaging patterns (point-to-point, pub-sub, request-reply), and shows why traditional systems like RabbitMQ or RDBMS queues hit limits at scale.

### Module 2 — Introduction to Apache Kafka
Uses `AdminClient` to introspect a live cluster:
- Cluster ID (KRaft-generated UUID)
- Controller broker identity
- Broker list with host/port/rack
- All user topics with partition count and replication factor

### Module 3 — Cluster Setup (KRaft edition)
- Explains the KRaft single-node startup sequence with exact commands
- Creates the demo topic `learnkarts-demo` (3 partitions, RF=1) via `AdminClient`
- Describes the topic's partition→leader→ISR mapping
- Shows Kafka 4.x CLI equivalents (no `--zookeeper` flag)

### Module 4 — Producer Architecture and Configuration
Three send patterns on the same topic:

| Pattern | `acks` | Notes |
|---------|--------|-------|
| Fire-and-forget | `0` | Highest throughput, no durability |
| Synchronous | `1` | Blocks until leader ACK; prints offset |
| Async + callback | `all` | Idempotent producer, linger+batch tuning |

Key config knobs printed and explained: `batch.size`, `linger.ms`, `buffer.memory`, `compression.type`, `enable.idempotence`.

### Module 5 — Serialization and Partitioning
- **Custom `OrderEventSerializer`**: serializes a `record OrderEvent(orderId, region, amount)` to a compact CSV byte array and deserializes it back (round-trip verified).
- **Custom `RegionPartitioner`**: routes `APAC:*` keys to partition 0, `EMEA:*` to 1, `AMER:*` to 2 — demonstrates deterministic key-based routing.

### Module 6 — Consumer Architecture and Configuration
- Explains pull-model and consumer group partition assignment.
- `LoggingRebalanceListener` shows `onPartitionsAssigned` / `onPartitionsRevoked` lifecycle.
- **KIP-848 (Kafka 4.x)**: explains the new server-side incremental cooperative rebalance protocol (`group.protocol=CONSUMER`).
- Lists all consumer groups via `AdminClient.listConsumerGroups()`.

### Module 7 — Offset Management and Deserialization
Three offset strategies demonstrated:

| Strategy | Semantics | Code pattern |
|----------|-----------|--------------|
| `enable.auto.commit=true` | At-least-once (risk) | explained in text |
| `commitSync()` after batch | At-least-once (controlled) | `manualCommitBatch()` |
| `commitSync(map)` per record + `seekToBeginning` | Replay demo | `manualCommitPerRecordWithSeek()` |

CLI commands shown for `kafka-consumer-groups.sh --describe` and `--reset-offsets`.

---

## Kafka 4.x highlights (vs 3.x)

| Area | Change |
|------|--------|
| ZooKeeper | **Removed entirely** — KRaft is the only mode |
| Java minimum | **17** (was 11 in 3.x); demo uses 21 |
| Consumer protocol | KIP-848 `CONSUMER` protocol is new default |
| CLI flags | `--zookeeper` flag removed from all tools |
| Metadata tool | `kafka-metadata-quorum.sh` replaces ZooKeeper-based inspection |
| Idempotence | `enable.idempotence=true` is now the **default** |
| `__consumer_offsets` | Retention and compaction settings unchanged |

---

## Useful Kafka 4.x CLI cheatsheet

```bash
KAFKA=~/kafka/bin

# Topic management
$KAFKA/kafka-topics.sh --bootstrap-server localhost:9092 --list
$KAFKA/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic learnkarts-demo

# Produce from terminal
$KAFKA/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic learnkarts-demo

# Consume from terminal
$KAFKA/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic learnkarts-demo --from-beginning

# Consumer group offsets
$KAFKA/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --describe --group learnkarts-demo-group

# KRaft quorum status
$KAFKA/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status
```
