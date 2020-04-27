package epl.pubsub.samosa.client.metrics;

import java.util.concurrent.atomic.AtomicLong;

public class ProducerMetrics{
    public AtomicLong aggregatePublishLatency;
    public AtomicLong maxPublishLatency;
    public AtomicLong numMessagesPublished;
    public AtomicLong aggregateTopicChangeLatency;
    public AtomicLong numTopicChanges;
    public AtomicLong maxTopicChangeLatency;

    public ProducerMetrics(){
        aggregatePublishLatency = new AtomicLong(0);
        maxPublishLatency = new AtomicLong(0);
        numMessagesPublished = new AtomicLong(0);
        aggregateTopicChangeLatency = new AtomicLong(0);
        numTopicChanges = new AtomicLong(0);
        maxTopicChangeLatency = new AtomicLong(0);
    }
}
