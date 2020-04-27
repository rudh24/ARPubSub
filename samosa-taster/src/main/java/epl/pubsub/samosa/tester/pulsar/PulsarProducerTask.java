package epl.pubsub.samosa.tester.pulsar;

import java.util.concurrent.atomic.AtomicBoolean;

import epl.pubsub.samosa.client.metrics.ProducerMetrics;
import epl.pubsub.samosa.client.pulsar.PulsarLocationProducer;
import epl.pubsub.samosa.tester.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarProducerTask implements Task {
    
    private final PulsarLocationProducer producer;
    private final String payload;
    private final int interval;
    private final String topic;
    private final AtomicBoolean isStarted = new AtomicBoolean();

    private static final Logger log = LoggerFactory.getLogger(PulsarProducerTask.class);

    private int numMessagesSent = 0;

    public PulsarProducerTask(PulsarLocationProducer producer, String payload, int interval, String topic){
        this.producer = producer;
        this.payload = payload;
        this.interval = interval;
        this.topic = topic;
        isStarted.set(false);
    }

    @Override
    public void run(){
        while(isStarted.get()){
            try {
                producer.send(payload); //.thenRun(()-> ++numMessagesSent);
                ++numMessagesSent;
                Thread.sleep(interval);
            } catch(InterruptedException e){
                e.printStackTrace();
            }
        }
    }

    public void start(){
        isStarted.set(true);
        producer.start(topic);
    }

    public void stop(){
        isStarted.set(false);
        producer.shutdown();
    }

    public double getAggregatedPublishLatency(){
        ProducerMetrics metrics = producer.getProducerMetrics();
        if(metrics.numMessagesPublished.get() == 0){
            return 0.0;
        }
        return (double)metrics.aggregatePublishLatency.get() / metrics.numMessagesPublished.get();
    }
    
    public long getNumMessagesPublished(){
        ProducerMetrics metrics = producer.getProducerMetrics();
        return metrics.numMessagesPublished.get();        
 
    }
    
    public double getSubscriptionChangeLatency(){
        ProducerMetrics metrics = producer.getProducerMetrics();
        if(metrics.numTopicChanges.get() == 0){
            return 0.0;
        }
        return (double)metrics.aggregateTopicChangeLatency.get() / metrics.numTopicChanges.get();
 
    }
}
