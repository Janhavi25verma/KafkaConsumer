# Kafka Consumer with Different Processing Strategies

This repository contains two different implementations of a Kafka consumer, each with its own approach to processing messages from multiple partitions. The implementations are available in two branches:

## Main Branch - Message Limit-Based Processing

In the main branch, the consumer processes messages from different partitions while adhering to a predefined message limit per partition. Once the limit is reached, the consumer stops processing that partition. However, when the consumer restarts, only the stopped partitions resume processing, while ongoing consumers remain unaffected.

### Key Characteristics:
- Messages are consumed up to a predefined limit per partition.
- Once the limit is reached, consumption for that partition stops.
- On consumer restart, only stopped partitions resume processing, while active ones continue.
- Ensures controlled processing and prevents unbounded message consumption.

## Second Branch - Batch Processing in One Second

In this branch, the consumer continuously processes messages in batches from different partitions. The batch size per partition varies dynamically, meaning:
- Partition 1 may process 2 messages,
- Partition 2 may process 3 messages,
- Partition 3 may process 4 messages,

This cycle repeats every second, ensuring a consistent flow of messages from multiple partitions.

### Key Characteristics:
- Continuous message consumption without stopping.
- Messages from different partitions are processed in varying batch sizes per second.
- Suitable for real-time stream processing scenarios.

## Advantages and Disadvantages

### Main Branch (Message Limit-Based Processing)

#### ✅ Advantages:
- Controlled message consumption per partition.
- Prevents overwhelming consumers by limiting the number of messages processed.
- Efficient for workloads where consumption should stop after a threshold is reached.

#### ❌ Disadvantages:
- If a partition reaches its limit, new messages won’t be consumed until the consumer restarts.
- Requires manual intervention or scheduled restarts for continuous processing.

### Second Branch (Batch Processing in One Second)

#### ✅ Advantages:
- Continuous and automated message processing without stopping.
- More efficient for real-time applications where messages need to be processed in a steady flow.
- Dynamic batch sizes allow optimal resource utilization.

#### ❌ Disadvantages:
- Unbounded consumption may lead to resource exhaustion if the message rate is too high.
- No built-in mechanism to pause or limit consumption dynamically.

## Use Cases

### Message Limit-Based Processing (Main Branch)
- Scenarios where message processing needs to stop after a certain threshold.
- Systems that require controlled data ingestion to avoid overloading downstream services.
- Event-driven architectures where partitions should not process indefinitely.

### Batch Processing in One Second (Second Branch)
- Real-time streaming applications that require continuous message ingestion.
- High-throughput data pipelines where dynamic batching improves efficiency.
- Use cases where messages must be processed at a steady rate from multiple partitions.

