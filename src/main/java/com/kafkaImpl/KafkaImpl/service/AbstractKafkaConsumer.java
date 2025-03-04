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


    //NEW LOGIC - process certain amount of messages as batches
    //we can pass multiple partition for single consumer also as takes map of partition limits and assigns consumer manually
    @Override
    public void run() {
        try {
            //always running stops only when stop method called via stop endpoint
            while (running) {
                // Fetch messages from Kafka every 1 second
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                // Map to store partition-wise batched messages
                Map<Integer, List<ConsumerRecord<String, String>>> partitionBatches = new HashMap<>();

                // Iterate through all fetched records -> message collection and batch creation
                for (ConsumerRecord<String, String> record : records) {
                    int partition = record.partition(); // Get the partition of the message

                    // Retrieve the processing limit for this partition; if not set, use a high default value
                    int limit = partitionLimits.getOrDefault(partition, Integer.MAX_VALUE);

                    // if list does not exist for particular partition then it is created
                    partitionBatches.putIfAbsent(partition, new ArrayList<>());

                    // Messages are added only if the partition has not exceeded its processing limit
                    //ensures once partition batch reaches its limit, additional messages are ignored in current poll cycle
                    //ignored messages remain in kafka topic and will be fetched in next poll.
                    if (partitionBatches.get(partition).size() < limit) {
                        partitionBatches.get(partition).add(record);
                    }
                }

                // Process messages partition-wise for a particular batch
                for (Integer partition : partitionBatches.keySet()) {
                    //get message for particular partition if multiple partition passed in one consumer
                    List<ConsumerRecord<String, String>> batch = partitionBatches.get(partition);

                    // Process each message in the batch
                    for (ConsumerRecord<String, String> record : batch) {
                        processMessage(record);
                    }

                    // Commit offsets after processing the batch
                    if (!batch.isEmpty()) { // Ensure there's at least one message before committing
                        long lastOffset = batch.get(batch.size() - 1).offset(); // Get the last message offset

                        // Manually commit the offset for the last processed message
                        consumer.commitSync(Collections.singletonMap(
                                new TopicPartition(topic, partition),
                                new OffsetAndMetadata(lastOffset + 1)
                        ));
                    }
                }

            }
        } catch (Exception e) {
            System.err.println("Consumer error: " + e.getMessage()); // Log any errors
            e.printStackTrace(); // Print stack trace for debugging
        } finally {
            consumer.close(); // Ensure consumer is closed to release resources
            System.out.println("Consumer stopped."); // Log consumer shutdown
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


