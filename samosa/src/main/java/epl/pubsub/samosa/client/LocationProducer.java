package epl.pubsub.samosa.client;

import epl.pubsub.samosa.client.metrics.ProducerMetrics;
import epl.pubsub.samosa.location.SubscriptionChangedCallback;

abstract public class LocationProducer implements SubscriptionChangedCallback<String> {
    public abstract ProducerMetrics getProducerMetrics();
}
