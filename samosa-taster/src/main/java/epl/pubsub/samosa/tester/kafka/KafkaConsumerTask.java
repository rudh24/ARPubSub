package epl.pubsub.samosa.tester.kafka;

import epl.pubsub.samosa.client.kafka.KafkaLocationConsumer;
import epl.pubsub.samosa.tester.Task;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaConsumerTask implements Task {

    private final KafkaLocationConsumer<Long, String> kafkaLocationConsumer;
    private final AtomicBoolean isStarted;
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerTask.class);
    List<String> initialTopics;

    public KafkaConsumerTask(KafkaLocationConsumer<Long, String> kafkaLocationConsumer, List<String> initialTopics) {
        this.kafkaLocationConsumer = kafkaLocationConsumer;
        this.initialTopics = initialTopics;
        isStarted = new AtomicBoolean(true);
    }

    @Override
    public void run() {
        int noRecordsCount = 0;
        int giveUp = 10;
        Long pollTimeout = 1000L;
        while (isStarted.get()) {
            final ConsumerRecords<Long, String> consumerRecords =
                    kafkaLocationConsumer.dynamicPoll(pollTimeout);

            if (consumerRecords.count()==0) {
                logger.error("No records received.");
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }
            logger.debug(String.format("Total Consumer Records: [%d]", consumerRecords.count()));
            consumerRecords.forEach(kafkaLocationConsumer::updateMetrics);

            //De-comment if needed for verbose output.
//            consumerRecords.forEach(record ->
//                    System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
//                            record.key(), record.value(),
//                            record.partition(), record.offset())
//            );

            kafkaLocationConsumer.commitAsync();
        }
        kafkaLocationConsumer.close();
    }

    public void start() {
        isStarted.set(true);
        kafkaLocationConsumer.subscribe(initialTopics);
    }

    public void stop() {
        isStarted.set(false);
    }
}
