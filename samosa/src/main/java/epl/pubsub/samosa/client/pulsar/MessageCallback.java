package epl.pubsub.samosa.client.pulsar;

import org.apache.pulsar.client.api.Message;

public interface MessageCallback {
    void onMessageReceived(Message<byte[]> payload);
}
