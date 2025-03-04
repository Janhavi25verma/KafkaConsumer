//@Service
//@RequiredArgsConstructor
//public class KafkaConsumerService {
//
//    @Value("${kafka.topic.name}")
//    private String topic;
//
//    private final KafkaConsumer<String, String> consumer;
//
//
//    public void consumeMessages() {
//        //how many messages each partition should consume
//        Map<Integer, Integer> partitionLimits = new HashMap<>();
//        partitionLimits.put(0, 8);  // Partition 0 -> consumes 8 messages
//        partitionLimits.put(1, 3);  // Partition 1 -> consumes 3 messages
//        partitionLimits.put(2, 5);  // Partition 2 -> consumes 5 messages
//
//        // creating new instance of TopicPartition class (represents a specific partition)
//        //storing instatnces in List -> to not rely on kafka automatic partition assignment.
//        List<TopicPartition> partitions = new ArrayList<>(Arrays.asList(
//                new TopicPartition(topic, 0),
//                new TopicPartition(topic, 1),
//                new TopicPartition(topic, 2)
//        ));
//
//        consumer.assign(partitions);  // Assign the specified partitions to this consumer
//
//        // Keeps track of how many messages have been consumed from each partition
//        Map<Integer, Integer> partitionMessageCount = new HashMap<>();
//        for (int partition : partitionLimits.keySet()) {
//            partitionMessageCount.put(partition, 0);  // Initialize counter for each partition
//        }
//
//        boolean consuming = true;
//
//        while (consuming) {
//            // Poll messages from Kafka with a timeout of 100ms -> returns empty if no message arrives after 100ms
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
//
//            for (ConsumerRecord<String, String> record : records) {
//                int partition = record.partition();  // Get partition number of the message
//
//                // If the partition is not in the limit map, ignore it
//                if (!partitionLimits.containsKey(partition)) continue;
//
////                // Increment the counter for the partition
////                partitionMessageCount.put(partition, partitionMessageCount.get(partition) + 1);
//
//                // Increment message count for this partition
//                partitionMessageCount.put(partition, partitionMessageCount.getOrDefault(partition, 0) + 1);
//
//
//                // Print the consumed message and its partition
//                System.out.println("Consumed from partition " + partition + ": " + record.value());
//
////                // Check if the limit for this partition has been reached
////                if (partitionMessageCount.get(partition) >= partitionLimits.get(partition)) {
////                    partitions.remove(new TopicPartition(topic, partition));  // Remove from active partitions
////                }
//
//                // Check if partition limit is reached
//                if (partitionMessageCount.get(partition) >= partitionLimits.getOrDefault(partition, Integer.MAX_VALUE)) {
//                    System.out.println("Pausing partition " + partition + " as limit reached.");
//                    consumer.pause(Collections.singleton(new TopicPartition(record.topic(), partition)));
//                }
//
//            }
//
//            //commiting latest processed message to kafka -> when consumer
//            // restarts it does not re-read same messages
//            consumer.commitSync();
//
//            // Stop consuming if all partitions have reached their limits
//            if (partitions.isEmpty()) {
//                consuming = false;
//            }
//        }
//
//        // consumer stops/closes after all partitions are processed
//        consumer.close();
//        System.out.println("All partitions reached their limit. Stopping consumer.");
//    }
//}

