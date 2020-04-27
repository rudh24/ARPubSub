package epl.pubsub.samosa.client.pulsar;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;

public class PulsarLocationClientBuilder {

    public static PulsarLocationClient getPulsarLocationClient(String configFile){
        try {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            mapper.findAndRegisterModules();
            PulsarConfig config = mapper.readValue(new File(configFile), PulsarConfig.class);
            PulsarLocationClient client = new PulsarLocationClientImpl();
            client.initClient(config);
            return client;
        }
        catch(IOException ex){
            ex.printStackTrace();
        }
        return null;
    }
}
