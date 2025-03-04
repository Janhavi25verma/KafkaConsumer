package com.kafkaImpl.KafkaImpl.service;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

public abstract class AbstractKafkaConsumer implements Runnable {


    private final KafkaConsumer<String, String> consumer;
    private final String topic;
    private final Map<Integer, Integer> partitionLimits;
    private final Map<Integer, Integer> partitionMessageCount = new HashMap<>();
    private volatile boolean running = true;

    public AbstractKafkaConsumer(String topic, Map<Integer, Integer> partitionLimits) {
        this.consumer = new KafkaConsumer<>(getProperties()); // Directly setting Kafka properties
        this.topic = topic;
        this.partitionLimits = partitionLimits;

        partitionLimits.keySet().forEach(p -> partitionMessageCount.put(p, 0));

        // Assigning consumer only to the partition the partitions specified in `partitionLimits`
        List<TopicPartition> partitions = new ArrayList<>();
        partitionLimits.keySet().forEach(p -> partitions.add(new TopicPartition(topic, p)));
        consumer.assign(partitions);

    }

    private Properties getProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "my-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "false"); // Manual offset commit
        props.put("auto.offset.reset", "latest");
        return props;
    }

    @Override
    public void run() {

        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    int partition = record.partition();// Get the partition of the current record

                    // If partition is not in the tracking map, ignore it
                    if (!partitionLimits.containsKey(partition)) continue;

                    int currentCount = partitionMessageCount.getOrDefault(partition, 0);

                    // If the count exceeds the defined limit for this partition, skip processing
                    if (currentCount >= partitionLimits.get(partition)) continue; // Skip if limit reached


                    processMessage(record);

                    // Update the count of messages processed for this partition
                    partitionMessageCount.put(partition, currentCount + 1);

                    // Manually commit the offset for the processed message
                    consumer.commitSync(Collections.singletonMap(
                            new TopicPartition(topic, partition),
                            new OffsetAndMetadata(record.offset() + 1)
                    ));


                    // If the partition has reached its message processing limit, pause it
                    if (partitionMessageCount.get(partition) >= partitionLimits.get(partition)) {
                        System.out.println("Pausing partition " + partition);
                        consumer.pause(Collections.singleton(new TopicPartition(topic, partition)));
                    }
                }

                // Check if all partitions have reached their respective limits
                boolean allPartitionsPaused = partitionMessageCount.entrySet().stream()
                        .allMatch(entry -> entry.getValue() >= partitionLimits.get(entry.getKey()));

                // If all partitions have reached their limits, stop the consumer gracefully
                if (allPartitionsPaused) {
                    System.out.println("All partitions reached limit. Stopping consumer.");
                    running = false; // Break out of the while loop
                }
            }
        } catch (Exception e) {
            System.err.println("Consumer encountered an error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Ensure consumer is closed to release resources
            consumer.close();
            System.out.println("Consumer stopped.");
        }
    }

    protected abstract void processMessage(ConsumerRecord<String, String> record);

    public void stop() {
        running = false;
        try {
            Thread.currentThread().join(); // Ensure the thread properly shuts down before restarting
        } catch (InterruptedException e) {
            System.err.println("Error stopping consumer: " + e.getMessage());
        }
    }

}


//package com.kafkaImpl.KafkaImpl.service;
//
//
//import org.apache.kafka.clients.consumer.Consumer;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.common.TopicPartition;
//import org.springframework.kafka.core.ConsumerFactory;
//
//import java.time.Duration;
//import java.util.*;
//
//    public abstract class AbstractKafkaConsumer implements Runnable {
//
//        private final Consumer<String, String> consumer;  // Kafka Consumer instance
//        private final String topic;  // Kafka topic name
//        private final Map<Integer, Integer> partitionLimits;  // Partition-specific message limits
//        private final Map<Integer, Integer> partitionMessageCount = new HashMap<>();  // Tracks message count per partition
//        private volatile boolean running = true;  // Flag to control consumer execution
//
//        public AbstractKafkaConsumer(ConsumerFactory<String, String> consumerFactory, String topic, Map<Integer, Integer> partitionLimits) {
//            this.consumer = consumerFactory.createConsumer();
//            this.topic = topic;
//            this.partitionLimits = partitionLimits;
//
//            // Initialize partition message counters
//            partitionLimits.keySet().forEach(p -> partitionMessageCount.put(p, 0));
//        }
//
//        @Override
//        public void run() {
//            // Assign specific partitions to this consumer (manual assignment)
//            List<TopicPartition> partitions = new ArrayList<>();
//            partitionLimits.keySet().forEach(p -> partitions.add(new TopicPartition(topic, p)));
//            consumer.assign(partitions);
//
//            while (running) {
//                // Poll messages from Kafka with a 100ms timeout
//                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
//
//                for (ConsumerRecord<String, String> record : records) {
//                    int partition = record.partition();  // Get partition of the message
//
//                    // Ignore messages from partitions not in the limit map
//                    if (!partitionLimits.containsKey(partition)) continue;
//
//                    // Increment message count for the partition
//                    partitionMessageCount.put(partition, partitionMessageCount.getOrDefault(partition, 0) + 1);
//
//                    // Process the message using the implemented logic in child classes
//                    processMessage(record);
//
//                    // Check if partition limit is reached and pause it
//                    if (partitionMessageCount.get(partition) >= partitionLimits.getOrDefault(partition, Integer.MAX_VALUE)) {
//                        System.out.println("Pausing partition " + partition);
//                        consumer.pause(Collections.singleton(new TopicPartition(topic, partition)));
//                    }
//                }
//
//                // Commit offsets to avoid re-reading processed messages
//                consumer.commitSync();
//
//                // Stop if all partitions have reached their limits
//                if (partitionMessageCount.values().stream().allMatch(count -> count >= partitionLimits.getOrDefault(count, Integer.MAX_VALUE))) {
//                    stop();
//                }
//            }

// Close consumer when finished
//            consumer.close();
//            System.out.println("Consumer stopped.");
//        }
//
//        // Abstract method to be implemented by specific consumers
//        protected abstract void processMessage(ConsumerRecord<String, String> record);
//
//        // Stop consumer execution
//        public void stop() {
//            running = false;
//        }
//    }


