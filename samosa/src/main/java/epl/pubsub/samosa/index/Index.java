package epl.pubsub.samosa.index;

import java.util.List;

public interface Index {

    void createIndex(double minX, double minY, double maxX,double maxY, double incr);

    long getIndexSize();
    
    List<String> getNearestNeighbors(double x, double y);    

    String getStringValue(double x, double y);    
}
