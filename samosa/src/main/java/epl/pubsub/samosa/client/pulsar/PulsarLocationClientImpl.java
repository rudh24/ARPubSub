package epl.pubsub.samosa.client.pulsar;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import org.apache.pulsar.common.policies.data.TenantInfo;

import java.util.HashSet;
import java.util.List;
import java.util.Collections;


class PulsarLocationClientImpl implements PulsarLocationClient {

    private PulsarClient client;
    private PulsarAdmin adminClient;
    
    private PulsarConfig config;
    
    @Override
    public void initClient(PulsarConfig config){
        this.config = config;

        ClientBuilder clientBuilder = PulsarClient.builder()
                                    .ioThreads(config.numIOThreads)
                                    .connectionsPerBroker(config.connectionsPerBroker)
                                    .serviceUrl(config.serviceUrl)
                                    .maxConcurrentLookupRequests(config.maxConcurrentLookups)
                                    .maxLookupRequests(config.maxLookups)
                                    .listenerThreads(config.listenerThreads);
        try {                        
            client = clientBuilder.build();

            PulsarAdminBuilder pulsarAdminBuilder = PulsarAdmin.builder().serviceHttpUrl(config.httpUrl);
            adminClient = pulsarAdminBuilder.build();

            try {
                String tenant = config.namespace.split("/")[0];
                HashSet<String> clusterSet = new HashSet<>();
                clusterSet.add(config.cluster);
                if(!adminClient.tenants().getTenants().contains(tenant)){
                    adminClient.tenants().createTenant(tenant, new TenantInfo(Collections.emptySet(), clusterSet));
                }
                adminClient.namespaces().createNamespace(config.namespace);
            } catch(PulsarAdminException e){
                e.printStackTrace();
                System.out.println(e.getMessage());
            } 
        } catch(PulsarClientException ex){
            ex.printStackTrace();
            System.out.println(ex.getMessage());
        }
    }

    @Override
    public PulsarLocationProducer getNewProducer(){
        return new PulsarLocationProducer(client, config.producerConfig, getTopicPrefix());
    }    
    
    @Override
    public PulsarLocationConsumer getNewConsumer(){
        return new PulsarLocationConsumer(client, getTopicPrefix());
    }

    @Override
    public void createTopics(List<String> topics){
        if(config.partitions ==1){
            return;
        }
        try {
            for(String topic: topics){
                
                adminClient.topics().createPartitionedTopic(topic, config.partitions);
            }
        } catch(PulsarAdminException ex){
            ex.printStackTrace();
            System.out.println(ex.getMessage());
        }
    }

    @Override
    public String getTopicPrefix(){
        return config.topicType+"://" + config.namespace;
    }

}
