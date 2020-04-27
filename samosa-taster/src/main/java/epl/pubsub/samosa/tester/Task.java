package epl.pubsub.samosa.tester;

public interface Task extends Runnable {
    void start();
    void stop();
}
