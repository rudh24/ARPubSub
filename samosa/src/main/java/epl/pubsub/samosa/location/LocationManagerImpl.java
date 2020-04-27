package epl.pubsub.samosa.location;

import java.util.concurrent.atomic.AtomicBoolean;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class LocationManagerImpl implements LocationManager, Runnable {
    
    String currentTopic;
    String newTopic;
    
    LocationChangedCallback callback;

    AtomicBoolean isStarted = new AtomicBoolean();

    long interval;
    
    String routeFile;
    
    public LocationManagerImpl(long changeIntervalMillis, String filename) {
        this.interval = changeIntervalMillis;
        routeFile = filename;
    }
    
    @Override
    public void initManager(LocationChangedCallback callback){
        this.callback = callback;
        isStarted.set(false);
    }

    @Override 
    public void stop(){
        isStarted.set(false);
    }

    @Override
    public void start() {
        isStarted.set(true);
    }
    
    
    @Override
    public void monitorLocation(){
        int count = 0;
        if(isStarted.get()){
            try {
                BufferedReader br = new BufferedReader(new FileReader(routeFile)); 
                 
                double x = 0.0, y = 0.0;
                String st;
                while ((st = br.readLine()) != null) {
                    double curX = Double.parseDouble(st.split(",")[0]);
                    double curY = Double.parseDouble(st.split(",")[1]);
                    if(x != curX || y != curY){
                        Location oldLocation = new Location(x, y); 
                        Location newLocation = new Location(curX, curY);
                        callback.onLocationChange(oldLocation, newLocation);
                    
                    }
                    x = curX;
                    y = curY;
                    try {
                        Thread.sleep(interval);
                    } catch(InterruptedException ex){
                        ex.printStackTrace();
                    }
                    if(!isStarted.get()){
                        br.close();
                        break;
                    } 
                }
            } catch(IOException ex) {
                System.out.println(ex.getMessage());
            }
        }
    }
   
    
    @Override
    public void run(){
        monitorLocation();
    }
}
