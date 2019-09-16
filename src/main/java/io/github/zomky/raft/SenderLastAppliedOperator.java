package io.github.zomky.raft;

import io.github.zomky.listener.LastAppliedListener;
import io.github.zomky.storage.RaftStorage;
import io.github.zomky.storage.log.entry.CommandEntry;
import io.github.zomky.storage.log.entry.IndexedLogEntry;
import io.github.zomky.utils.NettyUtils;
import io.rsocket.Payload;
import io.rsocket.util.ByteBufPayload;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;
import reactor.core.publisher.Operators;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class SenderLastAppliedOperator extends FluxOperator<Payload, Payload> {

    private RaftGroup raftGroup;
    private RaftStorage raftStorage;

    SenderLastAppliedOperator(Publisher<? extends Payload> source, RaftGroup raftGroup, RaftStorage raftStorage) {
        super(Flux.from(source));
        this.raftGroup = raftGroup;
        this.raftStorage = raftStorage;
    }

    @Override
    public void subscribe(CoreSubscriber<? super Payload> actual) {
        source.subscribe(new PublishLastAppliedSubscriber(actual, raftGroup, raftStorage));
    }

    private static class PublishLastAppliedSubscriber implements CoreSubscriber<Payload>, Subscription {

        enum SubscriberState {
            INIT,
            ACTIVE,
            OUTBOUND_DONE,
            COMPLETE
        }

        private final AtomicReference<SubscriberState> state = new AtomicReference<>(SubscriberState.INIT);
        private final AtomicReference<Throwable> firstException = new AtomicReference<Throwable>();

        private Subscriber<? super Payload> subscriber;
        private RaftGroup raftGroup;
        private RaftStorage raftStorage;
        private LastAppliedListener lastAppliedListener;
        private final Set<Long> inFlight = ConcurrentHashMap.newKeySet();
        private Subscription subscription;

        public PublishLastAppliedSubscriber(Subscriber<? super Payload> subscriber, RaftGroup raftGroup, RaftStorage raftStorage) {
            this.subscriber = subscriber;
            this.raftGroup = raftGroup;
            this.raftStorage = raftStorage;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            if (Operators.validate(this.subscription, subscription)) {
                lastAppliedListener = (index, response) -> {
                    try {
                        boolean removed = inFlight.remove(index);
                        if (removed) {
                            subscriber.onNext(ByteBufPayload.create(response));
                        }
                        if (inFlight.size() == 0) {
                            maybeComplete();
                        }
                    } catch (Exception e) {
                        handleError(e);
                    }
                };
                raftGroup.addLastAppliedListener(lastAppliedListener);
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
                byte[] value = NettyUtils.toByteArray(payload.sliceData());
                payload.release();
                CommandEntry commandEntry = new CommandEntry(raftStorage.getTerm(), System.currentTimeMillis(), value);
                raftGroup.appendLock();
                try {
                    IndexedLogEntry logEntryInfo = raftStorage.append(commandEntry);
                    inFlight.add(logEntryInfo.getIndex());
                    if (raftGroup.quorum() == 1) {
                        raftGroup.setCommitIndex(logEntryInfo.getIndex());
                    }
                    // maximum 256 in-flight entries
                    raftGroup.markNewEntry(logEntryInfo.getIndex(), logEntryInfo.getLogEntry().getTimestamp());
                } finally {
                    raftGroup.appendUnlock();
                }
            } catch (Exception e) {
                handleError(e);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (state.compareAndSet(SubscriberState.ACTIVE, SubscriberState.COMPLETE) ||
                    state.compareAndSet(SubscriberState.OUTBOUND_DONE, SubscriberState.COMPLETE)) {
                raftGroup.removeLastAppliedListener(lastAppliedListener);
                subscriber.onError(throwable);
            } else if (firstException.compareAndSet(null, throwable) && state.get() == SubscriberState.COMPLETE) {
                Operators.onErrorDropped(throwable, currentContext());
            }
        }

        @Override
        public void onComplete() {
            if (state.compareAndSet(SubscriberState.ACTIVE, SubscriberState.OUTBOUND_DONE) && inFlight.isEmpty()) {
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
                raftGroup.removeLastAppliedListener(lastAppliedListener);
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

        private  <T> boolean checkComplete(T t) {
            boolean complete = state.get() == SubscriberState.COMPLETE;
            if (complete && firstException.get() == null) {
                Operators.onNextDropped(t, currentContext());
            }
            return complete;
        }
    }
}
