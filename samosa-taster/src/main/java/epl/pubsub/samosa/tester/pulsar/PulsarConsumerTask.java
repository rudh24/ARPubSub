package epl.pubsub.samosa.tester.pulsar;


import java.util.concurrent.atomic.AtomicBoolean;
import java.util.List;

import epl.pubsub.samosa.client.metrics.ConsumerMetrics;
import epl.pubsub.samosa.client.pulsar.MessageCallback;
import epl.pubsub.samosa.client.pulsar.PulsarLocationConsumer;
import epl.pubsub.samosa.tester.Task;
import org.apache.pulsar.client.api.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarConsumerTask implements Task {
    
    private final PulsarLocationConsumer consumer;
    private final AtomicBoolean isStarted = new AtomicBoolean();
    private final List<String> initialTopics;
    private final String subName;

    private static final Logger log = LoggerFactory.getLogger(PulsarConsumerTask.class);

    private final MessageCallback messageCallback;

    public PulsarConsumerTask(PulsarLocationConsumer consumer, List<String> topics, String subName){
        this.consumer = consumer;
        isStarted.set(false);
        this.subName = subName;
        this.initialTopics = topics;

        messageCallback = (Message<byte[]> payload) -> {
        };
    }

    @Override
    public void run(){
        try {
            while(isStarted.get()){
                int interval = 10;
                Thread.sleep(interval);
            }
        } catch(InterruptedException e){
            e.printStackTrace();
        }
    }
    
    public void start(){
        consumer.start(initialTopics, subName, messageCallback);
        isStarted.set(true);
    
    }

    public void stop(){
        isStarted.set(false);
        consumer.shutdown();
    }

    public double getAggregatedLatency(){
        ConsumerMetrics metrics = consumer.getConsumerMetrics();
        if(metrics.numMessagesConsumed.get() == 0){
            return 0.0;
        }
        return (double)metrics.aggregateEndToEndLatency.get() / metrics.numMessagesConsumed.get();
    }
    
    public long getNumMessagesReceived(){
        ConsumerMetrics metrics = consumer.getConsumerMetrics();
        return metrics.numMessagesConsumed.get();        
 
    }
    
    public double getSubscriptionChangeLatency(){
        ConsumerMetrics metrics = consumer.getConsumerMetrics();
        if(metrics.numTopicChanges.get() == 0){
            return 0.0;
        }
        return (double)metrics.aggregateTopicChangeLatency.get() / metrics.numTopicChanges.get();
    }
}
