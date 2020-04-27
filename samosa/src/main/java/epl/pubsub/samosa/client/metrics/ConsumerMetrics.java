package epl.pubsub.samosa.client.metrics;

import java.util.concurrent.atomic.AtomicLong;

public class ConsumerMetrics{
    public AtomicLong aggregateEndToEndLatency;
    public AtomicLong maxEndToEndLatency;
    public AtomicLong numMessagesConsumed;
    public AtomicLong aggregateTopicChangeLatency;
    public AtomicLong numTopicChanges;
    public AtomicLong maxTopicChangeLatency;

    public ConsumerMetrics(){
        aggregateEndToEndLatency = new AtomicLong(0);
        maxEndToEndLatency = new AtomicLong(0);
        numMessagesConsumed = new AtomicLong(0);
        aggregateTopicChangeLatency = new AtomicLong(0);
        numTopicChanges = new AtomicLong(0);
        maxTopicChangeLatency = new AtomicLong(0);
    }
}
