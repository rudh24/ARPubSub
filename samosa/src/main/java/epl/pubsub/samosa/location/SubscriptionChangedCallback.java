package epl.pubsub.samosa.location;

public interface SubscriptionChangedCallback<T>{

    void onSubscriptionChange(T oldVal, T newVal);
}
