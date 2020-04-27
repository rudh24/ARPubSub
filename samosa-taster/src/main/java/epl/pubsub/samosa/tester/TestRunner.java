package epl.pubsub.samosa.tester;

import com.fasterxml.jackson.databind.ObjectMapper;
import epl.pubsub.samosa.client.LocationConsumer;
import epl.pubsub.samosa.client.LocationProducer;
import epl.pubsub.samosa.client.metrics.ConsumerMetrics;
import epl.pubsub.samosa.client.metrics.ProducerMetrics;
import epl.pubsub.samosa.index.Index;
import epl.pubsub.samosa.index.IndexFactory;
import epl.pubsub.samosa.location.*;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class TestRunner {
    private final Config config;
    private final StringBuilder payload = new StringBuilder();
    private final Index index;

    ExecutorService consumerExecutor;
    ExecutorService producerExecutor;
    ExecutorService locationManagerExecutor;

    private static final Logger log = LoggerFactory.getLogger(TestRunner.class);


    public TestRunner(Config config){
        this.config = config;
        IndexConfig indexConfig = config.indexConfig;

        try{
            BufferedReader br = new BufferedReader(new FileReader(config.payloadFile));

            String st;
            while ((st = br.readLine()) != null){
                payload.append(st);
            }
        } catch(IOException ex){
            ex.printStackTrace();
        }

        Properties props = new Properties();
        index = IndexFactory.getInitializedIndex( indexConfig.minX, indexConfig.minY, indexConfig.maxX,
                indexConfig.maxY, indexConfig.blockSize, indexConfig.indexType, props);
        log.info("created index");

        consumerExecutor = Executors.newFixedThreadPool(config.numConsumers);
        producerExecutor = Executors.newFixedThreadPool(config.numProducers);
        locationManagerExecutor = Executors.newFixedThreadPool(config.numProducers + config.numConsumers);
    }


    public List<Task> getConsumerTasks(List<LocationConsumer> consumers){
        List<Task> consumerTasks = new ArrayList<>();
        List<String> topics = new ArrayList<>();
        String topicStr = "init_test";
        topics.add(topicStr);
        int count=0;
        for (LocationConsumer consumer : consumers) {
            Task consumerTask = TaskFactory.getConsumerTask(consumer, topics, topicStr+"sub"+count++);
            consumerTask.start();
            consumerExecutor.execute(consumerTask);
            consumerTasks.add(consumerTask);
        }
        return consumerTasks;
    }

    public List<Task> getProducerTasks(List<LocationProducer> producers){
        List<Task> producerTasks = new ArrayList<>();
        String topic = "init_test";
        for (LocationProducer producer : producers) {
            Task producerTask = TaskFactory.getProducerTask(producer, payload.toString(), 1, topic);
            producerTask.start();
            producerExecutor.execute(producerTask);
            producerTasks.add(producerTask);
        }
        return producerTasks;
    }

    public List<LocationSubscriptionHandler<String>> getProducerLocationHandlers(List<LocationProducer> producers, Index index){
        List<LocationSubscriptionHandler<String>> handlers = new ArrayList<>();
        for (LocationProducer producer : producers) {
            LocationSubscriptionHandlerSingleTopicImpl handler = new LocationSubscriptionHandlerSingleTopicImpl(index);
            handler.initSubscriptionChangedCallback(producer);
            handlers.add(handler);
        }
        return handlers;
    }

    public List<LocationSubscriptionHandler<List<String>>> getConsumerLocationHandlers(List<LocationConsumer> consumers, Index index){
        List<LocationSubscriptionHandler<List<String>>> handlers = new ArrayList<>();
        for (LocationConsumer consumer : consumers) {
            LocationSubscriptionHandlerMultiTopicImpl handler = new LocationSubscriptionHandlerMultiTopicImpl(index);
            handler.initSubscriptionChangedCallback(consumer);
            handlers.add(handler);
        }
        return handlers;
    }

    public <T> List<LocationManager> getLocationManagers(List<LocationSubscriptionHandler<T>> locationHandlers, List<String> trajectoryFiles){
        List<LocationManager> locationManagers = new ArrayList<>();
        for(int i = 0; i < locationHandlers.size(); ++i){
            LocationManagerImpl lm = new LocationManagerImpl(config.locationChangeInterval, trajectoryFiles.get(i));
            lm.initManager(locationHandlers.get(i));
            lm.start();
            locationManagerExecutor.execute(lm);
            locationManagers.add(lm);
        }
        return locationManagers;
    }

    public void runTest(){
        if(config.numProducers != config.producerTrajectoryFiles.size()){
            System.out.println("Number of producers and trajectories don't match");
            return;
        }
        if(config.numConsumers != config.consumerTrajectoryFiles.size()){
            System.out.println("Number of consumers and trajectories don't match");
            return;
        }
        StopWatch sw = new StopWatch();

        sw.start();
        List<LocationProducer> producers =
                ClientFactory.getProducers(config.driver, config.numProducers, config.driverConfigFile);
        sw.stop();
        log.info("created {} producers in {} ms", config.numProducers, sw.getTime());
        List<Task> producerTasks = getProducerTasks(Objects.requireNonNull(producers));
        log.info("created producer tasks");
        List<LocationSubscriptionHandler<String>> producerHandlers = getProducerLocationHandlers(producers, index);
        log.info("created subscription handlers for producers");
        List<LocationManager> producerLocationManagers = getLocationManagers(producerHandlers, config.producerTrajectoryFiles);
        log.info("Created producer location managers");

        sw.reset();
        sw.start();
        List<LocationConsumer> consumers =
                ClientFactory.getConsumers(config.driver, config.numConsumers, config.driverConfigFile);
        sw.stop();
        log.info("created {} consumers in {} ms", config.numConsumers, sw.getTime());
        List<Task> consumerTasks = getConsumerTasks(Objects.requireNonNull(consumers));
        log.info("created consumer tasks");
        List<LocationSubscriptionHandler<List<String>>> consumerHandlers = getConsumerLocationHandlers(consumers, index);
        log.info("created subscription handlers for consumers");
        List<LocationManager> consumerLocationManagers = getLocationManagers(consumerHandlers, config.consumerTrajectoryFiles);
        log.info("Created consumer location managers");

        try {
            Thread.sleep(10000);
        } catch(InterruptedException e){
            e.printStackTrace();
        }

        for (Task producerTask : producerTasks) {
            producerTask.stop();
        }
        for (LocationManager producerLocationManager : producerLocationManagers) {
            producerLocationManager.stop();
        }
        for (Task consumerTask : consumerTasks) {
            consumerTask.stop();
        }
        for (LocationManager consumerLocationManager : consumerLocationManagers) {
            consumerLocationManager.stop();
        }

        consumerExecutor.shutdown();
        producerExecutor.shutdown();
        locationManagerExecutor.shutdown();

        class PubSubMetrics{
            public List<ConsumerMetrics> consumerMetrics;
            public List<ProducerMetrics> producerMetrics;
            public int numConsumers;
            public int numProducers;
        }

        PubSubMetrics metrics = new PubSubMetrics();
        metrics.producerMetrics = new ArrayList<>();
        metrics.consumerMetrics = new ArrayList<>();
        metrics.numConsumers = consumers.size();
        metrics.numProducers = producers.size();

        for (LocationConsumer consumer : consumers) {
            metrics.consumerMetrics.add(consumer.getConsumerMetrics());
        }
        for (LocationProducer producer : producers) {
            metrics.producerMetrics.add(producer.getProducerMetrics());
        }
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.writeValue(new File(config.outputFile), metrics);
        }catch(IOException ex){
            ex.printStackTrace();
        }
    }
}