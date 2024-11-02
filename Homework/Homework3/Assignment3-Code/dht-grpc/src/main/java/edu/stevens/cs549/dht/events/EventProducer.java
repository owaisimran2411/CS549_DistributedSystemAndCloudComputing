package edu.stevens.cs549.dht.events;

import com.google.protobuf.Empty;
import edu.stevens.cs549.dht.rpc.Binding;
import edu.stevens.cs549.dht.rpc.Event;
import edu.stevens.cs549.dht.rpc.NodeBindings;
import io.grpc.stub.StreamObserver;

public class EventProducer implements IEventListener {

    /*
     * Wrap the production of streamed gRPC events with the EventListener interface.
     */

    private StreamObserver<Event> observer;

    private EventProducer(StreamObserver<Event> observer) {
        this.observer = observer;
    }

    public static EventProducer create(StreamObserver<Event> observer) {
        return new EventProducer(observer);
    }

    @Override
    public void onNewBinding(String key, String value) {
        // TODO DONE MAYBE: emit new binding event to listening client.
        Binding binding = Binding.newBuilder().setKey(key).setValue(value).build();
        observer.onNext(Event.newBuilder().setNewBinding(binding).build());

    }

    @Override
    public void onMovedBinding(String key) {
        // TODO DONE MAYBE: emit moved binding event to listening client.
        observer.onNext(Event.newBuilder().build());

    }

    @Override
    public void onClosed(String key) {
        observer.onCompleted();
    }

    @Override
    public void onError(String key, Throwable throwable) {
        observer.onError(throwable);
    }

}
