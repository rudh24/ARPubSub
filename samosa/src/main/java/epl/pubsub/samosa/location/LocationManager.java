package epl.pubsub.samosa.location;

public interface LocationManager {
    void monitorLocation();

    void initManager(LocationChangedCallback cb); 
    
    void start();
        
    void stop();
}
