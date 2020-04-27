package epl.pubsub.samosa.client.pulsar;

import java.util.List;

public interface PulsarLocationClient{

    PulsarLocationConsumer getNewConsumer();

    PulsarLocationProducer getNewProducer();

    void initClient(PulsarConfig config);
    
    void createTopics(List<String> topics);

    String getTopicPrefix();
    
}
