package epl.pubsub.samosa.client.kafka;

import epl.pubsub.samosa.client.LocationConsumer;
import epl.pubsub.samosa.client.metrics.ConsumerMetrics;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class KafkaLocationConsumer<K,V> extends LocationConsumer {
    private static final Logger log = LoggerFactory.getLogger(KafkaLocationProducer.class);

    private final List<String> currentTopics;

    // Create the consumer using props.
    final Consumer<K, V> consumer;

    //Metrics
    private final ConsumerMetrics consumerMetrics = new ConsumerMetrics();

    public KafkaLocationConsumer(Properties props) {
        // Create the consumer using props.
        consumer = new KafkaConsumer<>(props);
        currentTopics = new ArrayList<>();

    }

    public void unsubscribe() {
        consumer.unsubscribe();
        currentTopics.clear();
    }


    public void close() {
        consumer.close();
    }

    public void commitAsync() {
       consumer.commitAsync();
    }

    public void subscribe(List<String> topics) {
       consumer.subscribe(topics);
       currentTopics.addAll(topics);
    }

    public ConsumerRecords<K,V> dynamicPoll(Long timeout) {
        Set<String> subscribedTo = consumer.subscription();
        synchronized (currentTopics) {
            if(!subscribedTo.equals(new HashSet<>(currentTopics))) {
                consumer.unsubscribe();
                consumer.subscribe(currentTopics);
            }
        }
        return consumer.poll(timeout);
    }

    public void onSubscriptionChange(List<String> oldVal, List<String> newVal) {
        //Assumption single topic change. Simple in Kafka. Actual sub/unsub in poll, because maintain threadsafety.
        synchronized (currentTopics) {
            currentTopics.clear();
            currentTopics.addAll(newVal);
        }
    }

    public void updateMetrics(ConsumerRecord<K, V> record) {
        consumerMetrics.numMessagesConsumed.getAndIncrement();
        long latency = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis() - record.timestamp()) / 1000;
        consumerMetrics.aggregateEndToEndLatency.getAndAccumulate(latency, Long::sum);
        consumerMetrics.maxEndToEndLatency.getAndAccumulate(latency, Math::max);
    }

    public ConsumerMetrics getConsumerMetrics() {
        return consumerMetrics;
    }

}
