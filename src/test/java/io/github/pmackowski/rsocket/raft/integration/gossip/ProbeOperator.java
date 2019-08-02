package io.github.pmackowski.rsocket.raft.integration.gossip;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoOperator;
import reactor.core.publisher.Operators;

import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Backpressure not supported
 * @param <T>
 */
class ProbeOperator<T, C extends Collection<? super T>, I, P> extends MonoOperator<T, C> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProbeOperator.class);

    private final Publisher<? extends T> indirect;
    private final Publisher<I> indirectStart;
    private final Publisher<P> protocolPeriodEnd;

    ProbeOperator(Mono<? extends T> direct, Publisher<? extends T> indirect, Publisher<I> indirectStart, Publisher<P> protocolPeriodEnd) {
        super(direct);
        this.indirect = indirect;
        this.indirectStart = indirectStart;
        this.protocolPeriodEnd = protocolPeriodEnd;
    }

    @Override
    public void subscribe(CoreSubscriber<? super C> actual) {
        LOGGER.debug("ProbeOperator subscribe");
        @SuppressWarnings("unchecked")
        C result = (C) new CopyOnWriteArrayList<T>();
        IndirectSubscriber<T,C> indirectSubscriber = new IndirectSubscriber<>(actual, indirect, result);
        DirectSubscriber<T,C> directSubscriber = new DirectSubscriber<>(actual, indirectSubscriber, result);
        IndirectStartSubscriber<I> indirectStartSubscriber = new IndirectStartSubscriber<>(directSubscriber);
        ProtocolPeriodEndSubscriber<P> protocolPeriodEndSubscriber = new ProtocolPeriodEndSubscriber<>(directSubscriber);

        source.subscribe(directSubscriber);

        indirectStart.subscribe(indirectStartSubscriber);
        protocolPeriodEnd.subscribe(protocolPeriodEndSubscriber);
    }

    static final class ProtocolPeriodEndSubscriber<P> implements CoreSubscriber<P> {

        final DirectSubscriber<?,?> directSubscriber;
        boolean once;

        ProtocolPeriodEndSubscriber(DirectSubscriber<?,?> directSubscriber) {
            this.directSubscriber = directSubscriber;
        }

        @Override
        public void onSubscribe(Subscription s) {
            directSubscriber.setProtocolPeriodEnd(s);
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(P p) {
            onComplete();
        }

        @Override
        public void onError(Throwable t) {
            if (once) {
                return;
            }
            once = true;
            directSubscriber.onError(t);
        }

        @Override
        public void onComplete() {
            if (once) {
                return;
            }
            once = true;
            directSubscriber.protocolPeriodComplete();
        }
    }

    static final class IndirectStartSubscriber<I> implements CoreSubscriber<I> {

        final DirectSubscriber<?,?> directSubscriber;
        boolean once;

        IndirectStartSubscriber(DirectSubscriber<?,?> directSubscriber) {
            this.directSubscriber = directSubscriber;
        }

        @Override
        public void onSubscribe(Subscription s) {
            directSubscriber.setIndirectStart(s);
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(I i) {
            onComplete();
        }

        @Override
        public void onError(Throwable t) {
            if (once) {
                return;
            }
            once = true;
            directSubscriber.onError(t);
        }

        @Override
        public void onComplete() {
            if (once) {
                return;
            }
            once = true;
            directSubscriber.subscribeIndirect();
        }
    }

    static final class IndirectSubscriber<T, C extends Collection<? super T>> implements CoreSubscriber<T> {

        final CoreSubscriber<? super C> actual;
        final Publisher<? extends T> indirect;
        final C result;

        IndirectSubscriber(CoreSubscriber<? super C> actual, Publisher<? extends T> indirect, C result) {
            this.actual = actual;
            this.indirect = indirect;
            this.result = result;
        }

        @Override
        public void onSubscribe(Subscription s) {
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(T t) {
            LOGGER.info("IndirectSubscriber onNext {}", t);
            result.add(t);
        }

        @Override
        public void onError(Throwable t) {
            LOGGER.debug("IndirectSubscriber onError {}", t);
        }

        @Override
        public void onComplete() {
            LOGGER.debug("IndirectSubscriber onComplete");
        }

        void subscribe() {
            LOGGER.debug("IndirectSubscriber subscribe");
            indirect.subscribe(this);
        }
    }

    static final class DirectSubscriber<T, C extends Collection<? super T>> implements CoreSubscriber<T>, Subscription {

        final CoreSubscriber<? super C> actual;
        final IndirectSubscriber<T,C> indirectSubscriber;

        volatile Subscription direct;

        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<DirectSubscriber, Subscription> DIRECT =
                AtomicReferenceFieldUpdater.newUpdater(DirectSubscriber.class, Subscription.class, "direct");

        volatile Subscription indirect;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<DirectSubscriber, Subscription> INDIRECT =
                AtomicReferenceFieldUpdater.newUpdater(DirectSubscriber.class, Subscription.class, "indirect");


        AtomicBoolean directCompleted = new AtomicBoolean(false);
        final C result;

        DirectSubscriber(CoreSubscriber<? super C> actual, IndirectSubscriber<T,C> indirectSubscriber, C result) {
            this.actual = actual;
            this.indirectSubscriber = indirectSubscriber;
            this.result = result;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (!DIRECT.compareAndSet(this, null, s)) {
                s.cancel();
                if (direct != Operators.cancelledSubscription()) {
                    Operators.reportSubscriptionSet();
                }
            } else {
                LOGGER.debug("DirectSubscriber onSubscribe {}", s);
                actual.onSubscribe(this);
            }
        }

        @Override
        public void request(long n) {
            LOGGER.debug("DirectSubscriber request {}", n);
            direct.request(n);
        }

        @Override
        public void cancel() {
            LOGGER.debug("DirectSubscriber cancel");
            direct.cancel();
            indirect.cancel();
        }

        @Override
        public void onNext(T t) {
            LOGGER.debug("DirectSubscriber onNext {}", t);
            result.add(t);
        }

        @Override
        public void onError(Throwable t) {
            LOGGER.error("DirectSubscriber onError", t);
            if (direct == null) {
                if (DIRECT.compareAndSet(this, null, Operators.cancelledSubscription())) {
                    Operators.error(actual, t);
                    return;
                }
            }
            cancel();

            actual.onError(t);
        }

        @Override
        public void onComplete() {
            LOGGER.debug("DirectSubscriber onComplete");
            directCompleted.compareAndSet(false, true);
        }

        void subscribeIndirect() {
            if (directCompleted.compareAndSet(false, true)) {
                indirectSubscriber.subscribe();
            }
        }

        void setIndirectStart(Subscription s) {
            if (!INDIRECT.compareAndSet(this, null, s)) {
                s.cancel();
                if (indirect != Operators.cancelledSubscription()) {
                    Operators.reportSubscriptionSet();
                }
            }
        }

        void protocolPeriodComplete() {
            actual.onNext(result);

            cancel();

            actual.onComplete();
        }

        void setProtocolPeriodEnd(Subscription s) {

        }
    }
}
