package epl.pubsub.samosa.tester.kafka;

import epl.pubsub.samosa.client.kafka.KafkaLocationProducer;
import epl.pubsub.samosa.tester.Task;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaProducerTask implements Task {

    private final KafkaLocationProducer<Long, String> producer;
    private final String payload;
    private final int interval;
    private final String initialTopic;

    private final AtomicBoolean isStarted = new AtomicBoolean();

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerTask.class);


    public KafkaProducerTask(KafkaLocationProducer<Long, String> producer, String payload, int interval, String initialTopic) {
        this.producer = producer;
        this.payload = payload;
        this.interval = interval;
        this.initialTopic = initialTopic;
        isStarted.set(false);
    }

    @Override
    public void run(){
        while(isStarted.get()){
            try {
                final ProducerRecord<Long, String> producerRecord = new ProducerRecord<>(initialTopic, payload);
                Future<RecordMetadata> futureRecord = producer.send(producerRecord);
                RecordMetadata record;
                try {
//                    record = futureRecord.get();
//                    log.debug(String.format("Metadata record timestamp: [%d]",record.timestamp()));
                    producer.updateMetrics();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
//                log.info("sent message");
                Thread.sleep(interval);
            } catch(InterruptedException e){
                e.printStackTrace();
            }
        }
    }
    public void start() {
        isStarted.set(true);
        producer.start(initialTopic);
    }
    public void stop(){
        isStarted.set(false);
    }
}