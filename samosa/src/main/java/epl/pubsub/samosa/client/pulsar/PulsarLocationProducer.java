package epl.pubsub.samosa.client.pulsar;

import epl.pubsub.samosa.client.LocationProducer;
import epl.pubsub.samosa.client.metrics.ProducerMetrics;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import org.apache.commons.lang3.time.StopWatch;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.TimeUnit;
import java.util.function.LongBinaryOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarLocationProducer extends LocationProducer {
    private static final Logger log = LoggerFactory.getLogger(PulsarLocationProducer.class);
    
    private Producer<byte[]> currentProducer;
    private ProducerBuilder<byte[]> currentProducerBuilder;

    private Producer<byte[]> newProducer;
    private ProducerBuilder<byte[]> newProducerBuilder;

    private final PulsarProducerConfig config;

    private final PulsarClient client;

    private final String topicPrefix;

    private final ProducerMetrics producerMetrics = new ProducerMetrics();
    private boolean  disableMetrics = false;

    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicBoolean isTransitioning = new AtomicBoolean();
    private final LongBinaryOperator latencyAccumulator;
    private final LongBinaryOperator maxValTester;

    ExecutorService executor;

    public PulsarLocationProducer(PulsarClient client, PulsarProducerConfig config, String topicPrefix){
        this.client = client;
        this.config = config;
        this.topicPrefix = topicPrefix;
        isTransitioning.set(false);
        executor = Executors.newSingleThreadExecutor();
        latencyAccumulator = Long::sum;
        maxValTester = Math::max;
    }

    public void disableMetricCollection() {
        disableMetrics = true;
        log.info("disabled metrics");
    }

    private ProducerBuilder<byte[]> createProducerBuilder() {
        ProducerBuilder<byte[]> producerBuilder = client.newProducer()
                               .enableBatching(config.batchingEnabled)
                               .batchingMaxPublishDelay(config.maxBatchPublishDelay, TimeUnit.MILLISECONDS)
                               .blockIfQueueFull(config.blockIfQueueFull)
                               .maxPendingMessages(config.maxPendingMessages);
        return producerBuilder;
            
    }

    public void start(String topic){
        currentProducerBuilder = createProducerBuilder();
        try {
            currentProducer = currentProducerBuilder.topic(topic).create();
            newProducerBuilder = currentProducerBuilder;
            newProducer = currentProducer;
        }catch(PulsarClientException ex){
            System.out.println(ex.getMessage());
        }
    }

    public void shutdown(){
        try {
            currentProducer.flush();
            currentProducer.close();
            executor.shutdown();
        } catch(PulsarClientException ex){
            System.out.println(ex.getMessage());
        }
    }

    public void onSubscriptionChange(String oldTopic, String newTopic){
        log.info("received subscription change event frm {} to {}", oldTopic, newTopic);
        TopicSwitchTask task = new TopicSwitchTask();
        StopWatch sw = new StopWatch();
        sw.start();
        switchTopic(newTopic);
        sw.stop();
        if(!disableMetrics){
            producerMetrics.aggregateTopicChangeLatency.getAndAccumulate(sw.getTime(), latencyAccumulator);
            producerMetrics.numTopicChanges.getAndIncrement();   
            producerMetrics.maxTopicChangeLatency.getAndAccumulate(sw.getTime(), maxValTester);
        }
        executor.execute(task);
    }
    private class TopicSwitchTask implements Runnable {
        @Override
        public void run() {
            reclaimProducer(); 
        }
    } 
    private void switchTopic(String newTopic){
        isTransitioning.set(true);
        try{
            lock.lock();
            String currentTopic = topicPrefix + "/" + newTopic;
            newProducerBuilder = createProducerBuilder();
            newProducer = newProducerBuilder.topic(newTopic).create();
        }catch(PulsarClientException ex){
            System.out.println(ex.getMessage());
        }
        finally{
            lock.unlock();
        }
    }
    
    private void reclaimProducer(){
        try{
            currentProducer.flush();
            currentProducer.close();
            currentProducerBuilder = newProducerBuilder;
            currentProducer = newProducer;
            isTransitioning.set(false);
        } catch(PulsarClientException ex){
            System.out.println(ex.getMessage());
        }
    }
    
    public void send(String payload) {
        byte[] payloadBytes = payload.getBytes();
        StopWatch sw = new StopWatch();
        sw.start();
        if(!isTransitioning.get()){
            currentProducer.newMessage().value(payloadBytes).sendAsync().thenApply(msgId->null);
        }
        else {
            try{
                lock.lock();
                newProducer.newMessage().value(payloadBytes).sendAsync().thenApply(msgId->null);
            } finally{
                lock.unlock();
            }
        }
        sw.stop();
        if(!disableMetrics){ 
            producerMetrics.maxPublishLatency.getAndAccumulate(sw.getTime(), maxValTester);
            producerMetrics.numMessagesPublished.getAndIncrement();
            producerMetrics.aggregatePublishLatency.getAndAccumulate(sw.getTime(), latencyAccumulator);
        }
    }

    public ProducerMetrics getProducerMetrics(){
        return producerMetrics;
    }    
}
