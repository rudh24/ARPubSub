package epl.pubsub.samosa.location;

public interface LocationSubscriptionHandler<T> extends LocationChangedCallback {

    void initSubscriptionChangedCallback(SubscriptionChangedCallback<T> callback); 

}
