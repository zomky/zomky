package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.transport.protobuf.CommandRequest;
import io.github.pmackowski.rsocket.raft.storage.RaftStorage;
import io.github.pmackowski.rsocket.raft.storage.log.entry.CommandEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedLogEntry;
import io.github.pmackowski.rsocket.raft.utils.NettyUtils;
import io.rsocket.Payload;
import io.rsocket.util.ByteBufPayload;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;
import reactor.core.publisher.Operators;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

public class SenderLastAppliedOperator extends FluxOperator<Payload, Payload> {

    private DefaultRaftServer raftServer;
    private RaftStorage raftStorage;

    SenderLastAppliedOperator(Publisher<? extends Payload> source, DefaultRaftServer raftServer, RaftStorage raftStorage) {
        super(Flux.from(source));
        this.raftServer = raftServer;
        this.raftStorage = raftStorage;
    }

    @Override
    public void subscribe(CoreSubscriber<? super Payload> actual) {
        source.subscribe(new PublishLastAppliedSubscriber(actual, raftServer, raftStorage));
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
        private DefaultRaftServer node;
        private RaftStorage raftStorage;
        private final ConcurrentMap<Long, Payload> unconfirmed = new ConcurrentHashMap<>();
        private Subscription subscription;

        public PublishLastAppliedSubscriber(Subscriber<? super Payload> subscriber, DefaultRaftServer node, RaftStorage raftStorage) {
            this.subscriber = subscriber;
            this.node = node;
            this.raftStorage = raftStorage;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            if (Operators.validate(this.subscription, subscription)) {
                node.addLastAppliedListener((index, response) -> {
                    try {
                        Payload payload = unconfirmed.remove(index);
                        if (payload != null) {
                            subscriber.onNext(ByteBufPayload.create(response, payload.sliceData()));
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
                final CommandRequest commandRequest = CommandRequest.parseFrom(NettyUtils.toByteArray(payload.sliceData()));
                CommandEntry commandEntry = new CommandEntry(raftStorage.getTerm(), System.currentTimeMillis(), commandRequest.toByteArray());
                IndexedLogEntry logEntryInfo = raftStorage.append(commandEntry);
                if (node.quorum() == 1) {
                    node.setCommitIndex(logEntryInfo.getIndex());
                }
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

        private  <T> boolean checkComplete(T t) {
            boolean complete = state.get() == SubscriberState.COMPLETE;
            if (complete && firstException.get() == null) {
                Operators.onNextDropped(t, currentContext());
            }
            return complete;
        }
    }
}
