package kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class WebConsoleConsumer {

//    private Consumer<Long, String> consumer;
//    private final Map<String, Map<String, MessageHandler>> topicHandlers = Collections.synchronizedMap(new HashMap<>());
//    private AtomicBoolean consumerRunning = new AtomicBoolean(false);
//
//    public WebConsoleConsumer() {
//        this.consumer = WebConsoleConsumer.createConsumer();
//    }
//
//
//    public void addHandler(String topic, MessageHandler handler) {
//        synchronized (topicHandlers) {
//            if (!topicHandlers.containsKey(topic)) {
//                topicHandlers.put(topic, Collections.synchronizedMap(new HashMap<>()));
//                updateConsumer();
//            }
//            topicHandlers.get(topic).put(handler.id, handler);
//        }
//    }
//
//    public void removeHandler(String topic, String handlerId) {
//        synchronized (topicHandlers) {
//            if (topicHandlers.containsKey(topic)) {
//                topicHandlers.get(topic).remove(handlerId);
//                if (topicHandlers.get(topic).size() == 0) {
//                    topicHandlers.remove(topic);
//                    updateConsumer();
//                }
//            }
//        }
//    }
//
//    private void updateConsumer() {
//        Set<String> topics = topicHandlers.keySet();
//        consumer.subscribe(topics);
//        if (topics.size() == 0) {
//            stop();
//        } else {
//            start();
//        }
//    }
//
//    private void start() {
//        consumerRunning.set(true);
//        run();
//    }
//
//    private void stop() {
//        consumerRunning.set(false);
//    }
//
//    private void run() {
//        Duration timeout = Duration.ofMillis(1000);
//
//        int noMessageFound = 0;
//
//        while (consumerRunning.get()) {
//            ConsumerRecords<Long, String> consumerRecords = consumer.poll(timeout);
//            // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
//            if (consumerRecords.count() == 0) {
//                noMessageFound++;
//                if (noMessageFound > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
//                    // If no message found count is reached to threshold exit loop.
//                    break;
//                else
//                    continue;
//            }
//
//            // handle each record.
//            consumerRecords.forEach(this::handle);
//
//            // commits the offset of record to broker.
//            consumer.commitAsync();
//        }
//
//        // consumer.close();
//    }
//
//    private void close() {
//        consumer.close();
//    }
//
//    private void handle(ConsumerRecord<Long, String> record) {
//        String topic = record.topic();
//        topicHandlers.get(topic).values().forEach(handler -> handler.handle(record));
//    }
//
//    public String stat() {
//        return "topics = " + topicHandlers.keySet();
//    }
//
//    public static abstract class MessageHandler {
//        public final String id = UUID.randomUUID().toString();
//        public abstract void handle(ConsumerRecord<Long, String> record);
//    }
//
//    public static Consumer<Long, String> createConsumer() {
//        Properties props = new Properties();
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, IKafkaConstants.GROUP_ID_CONFIG);
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, IKafkaConstants.MAX_POLL_RECORDS);
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, IKafkaConstants.OFFSET_RESET_LATEST);
//        return new KafkaConsumer<>(props);
//    }
}
