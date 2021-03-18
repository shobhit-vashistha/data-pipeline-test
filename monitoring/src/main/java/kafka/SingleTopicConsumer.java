package kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;


public class SingleTopicConsumer implements Closeable, AutoCloseable {

    private final Consumer<Long, String> consumer;
    private final AtomicBoolean consumerRunning = new AtomicBoolean(false);
    private MessageHandler handler = null;

    public SingleTopicConsumer() {
        this.consumer = SingleTopicConsumer.createKafkaConsumer();
    }

    public void subscribe(String topic, MessageHandler handler) {
        this.handler = handler;
        consumer.subscribe(Collections.singletonList(topic));
    }

    public void unsubscribe() {
        consumer.unsubscribe();
    }

    public void start() {
        consumerRunning.set(true);
        run();
    }

    public void stop() {
        consumerRunning.set(false);
    }

    private void run() {
        Duration timeout = Duration.ofMillis(1000);

        int noMessageFound = 0;

        while (consumerRunning.get()) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(timeout);
            // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
            if (consumerRecords.count() == 0) {
                noMessageFound++;
                if (noMessageFound > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
                    // If no message found count is reached to threshold exit loop.
                    break;
                else
                    continue;
            }
            noMessageFound = 0;
            // handle each record.
            consumerRecords.forEach(this::handle);

            // commits the offset of record to broker.
            consumer.commitAsync();
        }

        closeConsumer();
    }

    private void handle(ConsumerRecord<Long, String> record) {
        handler.handle(record);
    }

    private void closeConsumer() {
        if (consumer != null) {
            try {
                consumer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() throws IOException {
        closeConsumer();
    }

    public static abstract class MessageHandler {
        public abstract void handle(ConsumerRecord<Long, String> record);
    }

    public static Consumer<Long, String> createKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, IKafkaConstants.GROUP_ID_CONFIG + "-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, IKafkaConstants.MAX_POLL_RECORDS);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, IKafkaConstants.OFFSET_RESET_LATEST);
        return new KafkaConsumer<>(props);
    }
}
