package rsocket.playground.raft;

import io.rsocket.Payload;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;
import rsocket.playground.raft.storage.LogEntryInfo;
import rsocket.playground.raft.storage.ZomkyStorage;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class SenderConfirmOperator extends FluxOperator<Payload, Payload> {

    private Node node;
    private ZomkyStorage zomkyStorage;

    SenderConfirmOperator(Flux<? extends Payload> source, Node node, ZomkyStorage zomkyStorage) {
        super(source);
        this.node = node;
        this.zomkyStorage = zomkyStorage;
    }

    @Override
    public void subscribe(CoreSubscriber<? super Payload> actual) {
        source.subscribe(new PublishConfirmSubscriber(actual, node, zomkyStorage));
    }

    private static class PublishConfirmSubscriber implements CoreSubscriber<Payload> {

        private Subscriber<? super Payload> subscriber;
        private Node node;
        private ZomkyStorage zomkyStorage;
        private final ConcurrentNavigableMap<Long, Payload> unconfirmed = new ConcurrentSkipListMap<>();
        private AtomicBoolean onComplete = new AtomicBoolean(false);

        public PublishConfirmSubscriber(Subscriber<? super Payload> subscriber, Node node, ZomkyStorage zomkyStorage) {
            this.subscriber = subscriber;
            this.node = node;
            this.zomkyStorage = zomkyStorage;
        }

        @Override
        public void onSubscribe(Subscription s) {
            node.addConfirmListener(index -> {
                ConcurrentNavigableMap<Long, Payload> unconfirmedToSend = unconfirmed.headMap(index, true);
                Iterator<Map.Entry<Long, Payload>> iterator = unconfirmedToSend.entrySet().iterator();
                while (iterator.hasNext()) {
                    subscriber.onNext(iterator.next().getValue());
                    iterator.remove();
                }
                if (onComplete.get() && unconfirmed.size() == 0) {
                    subscriber.onComplete();
                }
            });
            subscriber.onSubscribe(s);
        }

        @Override
        public void onNext(Payload payload) {
            LogEntryInfo logEntryInfo = zomkyStorage.appendLog(zomkyStorage.getTerm(), payload.getData());
            unconfirmed.putIfAbsent(logEntryInfo.getIndex(), payload);
        }

        @Override
        public void onError(Throwable throwable) {
            subscriber.onError(throwable);
        }

        @Override
        public void onComplete() {
            onComplete.set(true);
        }
    }
}