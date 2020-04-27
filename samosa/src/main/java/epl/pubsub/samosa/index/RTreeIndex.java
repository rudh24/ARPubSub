package epl.pubsub.samosa.index;

import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.geometry.Geometry;
import com.github.davidmoten.rtree.geometry.Geometries;

import org.apache.commons.lang3.RandomStringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import java.util.Properties;

class RTreeIndex implements Index{
    private static final Logger log = LoggerFactory.getLogger(RTreeIndex.class);

    private RTree<String, Geometry> rtreeIndex;
    private int numRandomChars;
    private double boundingBoxSize;

    public RTreeIndex(Properties props){
        rtreeIndex = RTree.create();
        numRandomChars = 10;
        boundingBoxSize = 0.002;
        if(props.containsKey("numRandomChars")){
            numRandomChars = (int)props.get("numRandomChars");
        }
        if(props.containsKey("boundingBoxSize")){
            boundingBoxSize = (double)props.get("boundingBoxSize");
        }
    }

    @Override
    public void createIndex(double minX, double minY, double maxX, double maxY, double incr){
        for(double x = minX; x <= maxX; x += incr){
            for(double y = minY; y <=maxY; y += incr){
                StringBuilder sb = new StringBuilder();
                sb.append("key_");
                sb.append(x);
                sb.append("_");
                sb.append(y);
                sb.append("_");
                sb.append(RandomStringUtils.random(numRandomChars));
                rtreeIndex = rtreeIndex.add(sb.toString(), Geometries.rectangleGeographic(y, x, y+incr, x+incr));
            }
        }
    
        log.info("Created rtree with " + rtreeIndex.size());
    }

    @Override 
    public String getStringValue(double x, double y){
        //TODO fix this method
        List<String> matches = getNearestNeighbors(x,y);
        if(matches.size() > 0){
            return matches.get(0);
        }
        return "";
    }

    @Override
    public List<String> getNearestNeighbors(double x, double y){
        
        Iterable<Entry<String, Geometry>> results = rtreeIndex.search(Geometries.rectangleGeographic(y, x, y + boundingBoxSize, x + boundingBoxSize)).toBlocking().toIterable();
        List<String> keys = new ArrayList<>();
        for(Entry<String, Geometry>e: results){
            keys.add(e.value());
        }
        return keys;
 
    }

    @Override
    public long getIndexSize(){
        return rtreeIndex.size();
    }    
}




