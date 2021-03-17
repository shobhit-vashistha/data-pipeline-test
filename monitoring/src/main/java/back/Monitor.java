package back;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import kafka.ConsumerCreator;
import kafka.IKafkaConstants;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


public class Monitor {
    public static final String KAFKA_INGESTION_TOPIC = "dev.telemetry.ingest";
    public static AtomicBoolean consumerRunning = new AtomicBoolean(false);

    public static void main(String[] args) {
        Collection<String> topicList = Collections.singletonList(KAFKA_INGESTION_TOPIC);

        Thread thread = new Thread(() -> runConsumer(topicList));
        thread.start();

        runProducer(KAFKA_INGESTION_TOPIC);
    }

    static void runConsumer(Collection<String> topicList) {
        Consumer<Long, String> consumer = ConsumerCreator.createConsumer(topicList);
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

            //print each record.
            consumerRecords.forEach(record -> {
                System.out.println("Record kafka.Topic " + record.topic());
                System.out.println("Record Key " + record.key());
                System.out.println("Record value " + record.value());
                System.out.println("Record partition " + record.partition());
                System.out.println("Record offset " + record.offset());
            });

            // commits the offset of record to broker.
            consumer.commitAsync();
        }
        consumer.close();
    }

    static void runProducer(String topic) {
        Producer<Long, String> producer = ProducerCreator.createProducer();

        for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
            ProducerRecord<Long, String> record = new ProducerRecord<>(topic, "This is record " + index);
            try {
                RecordMetadata metadata = producer.send(record).get();
                System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
                        + " with offset " + metadata.offset());
            } catch (ExecutionException e) {
                System.out.println("Error in sending record");
                System.out.println(e);
            } catch (InterruptedException e) {
                System.out.println("Error in sending record");
                System.out.println(e);
            }
        }
    }
}
