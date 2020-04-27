package epl.pubsub.samosa.location;

public interface LocationChangedCallback{

    void onLocationChange(Location oldLocation, Location newLocation);
}
