package kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaUtil {

    public AtomicBoolean consumerRunning = new AtomicBoolean(false);
    private final Map<String, Filter> topicFilterMap = new HashMap<>();
    private Consumer<Long, String> consumer = null;

    public KafkaUtil() {

    }

    public void subscribe(String topic, String filterString) {
        Filter filter = Filter.getFilter(filterString);
        subscribe(topic, filter);
    }

    public void subscribe(String topic, Filter filter) {
        topicFilterMap.put(topic, filter);
        updateConsumer();
    }

    public void unsubscribe(String topic) {
        topicFilterMap.remove(topic);
        updateConsumer();
    }

    public void updateConsumer() {

    }

    private void consumeAsync() {

    }

    public String stat() {
        return topicFilterMap.keySet().toString();
    }

    private Consumer<Long, String> getConsumer() {
        if (consumer == null) consumer = ConsumerCreator.createConsumer(topicFilterMap.keySet());
        return consumer;
    }



}
