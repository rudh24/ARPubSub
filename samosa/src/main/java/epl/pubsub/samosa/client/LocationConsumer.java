package epl.pubsub.samosa.client;

import epl.pubsub.samosa.client.metrics.ConsumerMetrics;
import epl.pubsub.samosa.location.SubscriptionChangedCallback;

import java.util.List;

public abstract class LocationConsumer implements SubscriptionChangedCallback<List<String>> {
    public abstract ConsumerMetrics getConsumerMetrics();
}
