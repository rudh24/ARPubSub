package epl.pubsub.samosa.client.pulsar;

public class PulsarProducerConfig {
    public boolean batchingEnabled;
    public int maxBatchPublishDelay;
    public boolean blockIfQueueFull;
    public int maxPendingMessages; 
}
