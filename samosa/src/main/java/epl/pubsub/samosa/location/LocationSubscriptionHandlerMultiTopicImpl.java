package epl.pubsub.samosa.location;

import java.util.List;
import java.util.Arrays;

import epl.pubsub.samosa.index.Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocationSubscriptionHandlerMultiTopicImpl implements  LocationSubscriptionHandler<List<String>> {
    private static final Logger log = LoggerFactory.getLogger(LocationSubscriptionHandlerMultiTopicImpl.class);

    private Index index;
    private SubscriptionChangedCallback<List<String>> callback;

    public LocationSubscriptionHandlerMultiTopicImpl(Index index){
        this.index = index;

    }
    public void initSubscriptionChangedCallback( SubscriptionChangedCallback<List<String>> callback) {
        this.callback = callback;
    }
    
    @Override
    public void onLocationChange(Location oldLocation, Location newLocation){
        log.info("multi topic location change");
        List<String> oldTopics = index.getNearestNeighbors(oldLocation.x, oldLocation.y);
        List<String> newTopics = index.getNearestNeighbors(newLocation.x, newLocation.y);
        log.info(Arrays.toString(oldTopics.toArray())+ "|" +Arrays.toString(newTopics.toArray()));
        if(!oldTopics.equals(newTopics)){
            log.info("making sub change");
            callback.onSubscriptionChange(oldTopics, newTopics);
        
        }
    }
    
}
