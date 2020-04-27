package epl.pubsub.samosa.tester;

import epl.pubsub.samosa.client.LocationConsumer;
import epl.pubsub.samosa.client.LocationProducer;
import epl.pubsub.samosa.client.kafka.KafkaLocationConsumer;
import epl.pubsub.samosa.client.kafka.KafkaLocationProducer;
import epl.pubsub.samosa.client.pulsar.PulsarLocationClient;
import epl.pubsub.samosa.client.pulsar.PulsarLocationClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ClientFactory {
    private static final Logger log = LoggerFactory.getLogger(ClientFactory.class);
    public enum Driver {
        KAFKA,
        PULSAR
    }

    private static PulsarLocationClient client;
    public static List<LocationConsumer> getConsumers(String driver, int numConsumers, String driverConfig) {

        switch (Driver.valueOf(driver)) {
            case KAFKA:
                return getKafkaConsumers(numConsumers, driverConfig);
            case PULSAR:
                return getPulsarConsumers(numConsumers, driverConfig);
            default:
                log.error("Unknown driver: {}", driver);
                return null;
        }
    }

    public static List<LocationProducer> getProducers(String driver, int numProducers, String driverConfig) {
        switch (Driver.valueOf(driver)) {
            case KAFKA:
                return getKafkaProducers(numProducers, driverConfig);
            case PULSAR:
                return getPulsarProducers(numProducers, driverConfig);
            default:
                log.error("Unknown driver: {}", driver);
                return null;
        }
    }

    private  static void setPulsarClient(String driverConfig) {
        if(client == null)  {
            client = PulsarLocationClientBuilder.getPulsarLocationClient(driverConfig);
        }
    }

    private static List<LocationProducer> getPulsarProducers(int numProducers, String driverConfig) {
        setPulsarClient(driverConfig);
        List<LocationProducer> producers = new ArrayList<>();
        for(int i = 0; i < numProducers; ++i){
            producers.add(client.getNewProducer());
        }
        return producers;
    }

    private static List<LocationConsumer> getPulsarConsumers(int numConsumers, String driverConfig) {
        setPulsarClient(driverConfig);
        List<LocationConsumer> consumers = new ArrayList<>();
        for(int i = 0; i < numConsumers; ++i){
            consumers.add(client.getNewConsumer());
        }
        return consumers;
    }

    private static List<LocationConsumer> getKafkaConsumers(int numConsumers, String driverConfig) {
        String bootstrapServers =
                "localhost:9092,localhost:9093,localhost:9094";
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        List<LocationConsumer> consumers = new ArrayList<>();
        for(int i = 0; i < numConsumers; ++i){
            consumers.add(new KafkaLocationConsumer<Long, String>(props));
        }
        return consumers;
    }

    private static List<LocationProducer> getKafkaProducers(int numProducers, String driverConfig){

        //Probably specify path for karka-props.yaml via config
        String bootstrapServers =
                "localhost:9092,localhost:9093,localhost:9094";
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        List<LocationProducer> producers = new ArrayList<>();
        for(int i = 0; i < numProducers; ++i){
            producers.add(new KafkaLocationProducer<Long, String>(props));
        }
        return producers;
    }

}
