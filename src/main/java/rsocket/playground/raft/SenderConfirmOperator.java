package rsocket.playground.raft;

import io.rsocket.Payload;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;
import reactor.core.publisher.Operators;
import rsocket.playground.raft.storage.LogEntryInfo;
import rsocket.playground.raft.storage.RaftStorage;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;

public class SenderConfirmOperator extends FluxOperator<Payload, Payload> {

    private DefaultRaftServer raftServer;
    private RaftStorage raftStorage;

    SenderConfirmOperator(Publisher<? extends Payload> source, DefaultRaftServer raftServer, RaftStorage raftStorage) {
        super(Flux.from(source));
        this.raftServer = raftServer;
        this.raftStorage = raftStorage;
    }

    @Override
    public void subscribe(CoreSubscriber<? super Payload> actual) {
        source.subscribe(new PublishConfirmSubscriber(actual, raftServer, raftStorage));
    }

    private static class PublishConfirmSubscriber implements CoreSubscriber<Payload> , Subscription {

        enum SubscriberState {
            INIT,
            ACTIVE,
            OUTBOUND_DONE,
            COMPLETE
        }

        private final AtomicReference<SubscriberState> state = new AtomicReference<>(SubscriberState.INIT);
        private final AtomicReference<Throwable> firstException = new AtomicReference<Throwable>();

        private Subscriber<? super Payload> subscriber;
        private DefaultRaftServer node;
        private RaftStorage raftStorage;
        private final ConcurrentNavigableMap<Long, Payload> unconfirmed = new ConcurrentSkipListMap<>();
        private Subscription subscription;

        public PublishConfirmSubscriber(Subscriber<? super Payload> subscriber, DefaultRaftServer node, RaftStorage raftStorage) {
            this.subscriber = subscriber;
            this.node = node;
            this.raftStorage = raftStorage;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            if (Operators.validate(this.subscription, subscription)) {
                node.addConfirmListener(index -> {
                    try {
                        ConcurrentNavigableMap<Long, Payload> unconfirmedToSend = unconfirmed.headMap(index, true);
                        Iterator<Map.Entry<Long, Payload>> iterator = unconfirmedToSend.entrySet().iterator();
                        // unconfirmed size is not greater than number of requested elements
                        while (iterator.hasNext()) {
                            subscriber.onNext(iterator.next().getValue());
                            iterator.remove();
                        }
                        if (unconfirmed.size() == 0) {
                            maybeComplete();
                        }
                    } catch (Exception e) {
                        handleError(e);
                    }
                });
                this.subscription = subscription;
                state.set(SubscriberState.ACTIVE);
                subscriber.onSubscribe(this);
            }
        }

        @Override
        public void onNext(Payload payload) {
            if (checkComplete(payload)) {
                return;
            }

            try {
                LogEntryInfo logEntryInfo = raftStorage.appendLog(raftStorage.getTerm(), payload.getData());
                unconfirmed.putIfAbsent(logEntryInfo.getIndex(), payload);
            } catch (Exception e) {
                handleError(e);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (state.compareAndSet(SubscriberState.ACTIVE, SubscriberState.COMPLETE) ||
                    state.compareAndSet(SubscriberState.OUTBOUND_DONE, SubscriberState.COMPLETE)) {
                subscriber.onError(throwable);
            } else if (firstException.compareAndSet(null, throwable) && state.get() == SubscriberState.COMPLETE) {
                Operators.onErrorDropped(throwable, currentContext());
            }
        }

        @Override
        public void onComplete() {
            if (state.compareAndSet(SubscriberState.ACTIVE, SubscriberState.OUTBOUND_DONE) && unconfirmed.size() == 0) {
                maybeComplete();
            }
        }

        @Override
        public void request(long n) {
            subscription.request(n);
        }

        @Override
        public void cancel() {
            subscription.cancel();
        }

        private void maybeComplete() {
            boolean done = state.compareAndSet(SubscriberState.OUTBOUND_DONE, SubscriberState.COMPLETE);
            if (done) {
                subscriber.onComplete();
            }
        }

        private void handleError(Exception e) {
            boolean complete = checkComplete(e);
            firstException.compareAndSet(null, e);
            if (!complete) {
                onError(e);
            }
        }

        private <T> boolean checkComplete(T t) {
            boolean complete = state.get() == SubscriberState.COMPLETE;
            if (complete && firstException.get() == null) {
                Operators.onNextDropped(t, currentContext());
            }
            return complete;
        }

    }
}