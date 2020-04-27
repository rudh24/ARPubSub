package epl.pubsub.samosa.client.pulsar;

import org.apache.pulsar.common.naming.TopicDomain;

public class PulsarConfig {
    public int numIOThreads;
    public int connectionsPerBroker;
    public int statsInterval;
    public String serviceUrl;
    public int maxConcurrentLookups;
    public int maxLookups;
    public int listenerThreads;
    public String httpUrl;
    public String namespace;
    public String cluster;
    public int partitions;
    public TopicDomain topicType  = TopicDomain.non_persistent;
    public PersistenceConfiguration persistence = new PersistenceConfiguration();
    
    public static class PersistenceConfiguration {
        public int ensembleSize = 3;    
        public int writeQuorum =3;
        public int ackQuorum = 2;
        public boolean deduplicationEnabled = false;
    }   

    public PulsarProducerConfig producerConfig;
}
