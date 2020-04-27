package epl.pubsub.samosa.tester;

import epl.pubsub.samosa.client.LocationConsumer;
import epl.pubsub.samosa.client.LocationProducer;
import epl.pubsub.samosa.client.kafka.KafkaLocationConsumer;
import epl.pubsub.samosa.client.kafka.KafkaLocationProducer;
import epl.pubsub.samosa.client.pulsar.PulsarLocationConsumer;
import epl.pubsub.samosa.client.pulsar.PulsarLocationProducer;
import epl.pubsub.samosa.tester.kafka.KafkaConsumerTask;
import epl.pubsub.samosa.tester.kafka.KafkaProducerTask;
import epl.pubsub.samosa.tester.pulsar.PulsarConsumerTask;
import epl.pubsub.samosa.tester.pulsar.PulsarProducerTask;

import java.util.List;


public class TaskFactory {

    @SuppressWarnings("unchecked")
    public static Task getConsumerTask(LocationConsumer consumer, List<String> topics, String subscriptionName) {
        if(consumer instanceof KafkaLocationConsumer) {
            return new KafkaConsumerTask((KafkaLocationConsumer<Long, String>) consumer, topics);
        }
        else if(consumer instanceof PulsarLocationConsumer) {
            return new PulsarConsumerTask((PulsarLocationConsumer) consumer, topics, subscriptionName);
        }
        else return null;
    }

    @SuppressWarnings("unchecked")
    public static  Task getProducerTask(LocationProducer producer, String payload, int interval, String initialTopic) {
        if(producer instanceof KafkaLocationProducer) {
            return new KafkaProducerTask((KafkaLocationProducer<Long, String>) producer, payload, interval, initialTopic);
        }
        else if(producer instanceof PulsarLocationProducer) {
            return new PulsarProducerTask((PulsarLocationProducer) producer, payload, interval, initialTopic);
        }
        else return null;
    }
}
