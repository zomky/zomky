package rsocket.playground.raft;

import io.rsocket.Payload;
import io.rsocket.util.ByteBufPayload;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;
import reactor.core.publisher.Operators;
import rsocket.playground.raft.storage.LogEntryInfo;
import rsocket.playground.raft.storage.ZomkyStorage;

import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class SenderLastAppliedOperator extends FluxOperator<Payload, Payload> {

    private Node node;
    private ZomkyStorage zomkyStorage;

    SenderLastAppliedOperator(Publisher<? extends Payload> source, Node node, ZomkyStorage zomkyStorage) {
        super(Flux.from(source));
        this.node = node;
        this.zomkyStorage = zomkyStorage;
    }

    @Override
    public void subscribe(CoreSubscriber<? super Payload> actual) {
        source.subscribe(new PublishLastAppliedSubscriber(actual, node, zomkyStorage));
    }

    private static class PublishLastAppliedSubscriber implements CoreSubscriber<Payload>, Subscription {

        private Subscriber<? super Payload> subscriber;
        private Node node;
        private ZomkyStorage zomkyStorage;
        private final ConcurrentNavigableMap<Long, Payload> unconfirmed = new ConcurrentSkipListMap<>();
        private Subscription subscription;

        public PublishLastAppliedSubscriber(Subscriber<? super Payload> subscriber, Node node, ZomkyStorage zomkyStorage) {
            this.subscriber = subscriber;
            this.node = node;
            this.zomkyStorage = zomkyStorage;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            if (Operators.validate(this.subscription, subscription)) {
                node.addLastAppliedListener((index, response) -> {
                    Payload payload = unconfirmed.remove(index);
                    if (payload != null) {
                        subscriber.onNext(ByteBufPayload.create(response, payload.sliceData()));
                    }
                    if (unconfirmed.size() == 0) {
                        subscriber.onComplete();
                    }
                });
                this.subscription = subscription;
                subscriber.onSubscribe(subscription);
            }
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
        }

        @Override
        public void request(long n) {
            subscription.request(n);
        }

        @Override
        public void cancel() {
            subscription.cancel();
        }
    }
}
