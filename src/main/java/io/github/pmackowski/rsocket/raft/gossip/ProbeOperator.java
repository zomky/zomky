package io.github.pmackowski.rsocket.raft.gossip;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoOperator;
import reactor.core.publisher.Operators;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Backpressure not supported
 * @param <T>
 */
class ProbeOperator<T, C extends ProbeOperatorResult<T>, I, P> extends MonoOperator<T, C> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProbeOperator.class);

    private final Publisher<? extends T> indirect;
    private final Publisher<I> indirectDelay;
    private final Publisher<P> probeTimeout;

    ProbeOperator(Mono<? extends T> direct, Publisher<? extends T> indirect, Publisher<I> indirectDelay, Publisher<P> probeTimeout) {
        super(direct);
        this.indirect = indirect;
        this.indirectDelay = indirectDelay;
        this.probeTimeout = probeTimeout;
    }

    @Override
    public void subscribe(CoreSubscriber<? super C> actual) {
        @SuppressWarnings("unchecked")
        C result = (C) new ProbeOperatorResult<>();
        IndirectSubscriber<T,C> indirectSubscriber = new IndirectSubscriber<>(actual, indirect, result);
        DirectSubscriber<T,C> directSubscriber = new DirectSubscriber<>(actual, indirectSubscriber, result);
        IndirectDelaySubscriber<I> indirectDelaySubscriber = new IndirectDelaySubscriber<>(directSubscriber);
        ProbeTimeoutSubscriber<P> probeTimeoutSubscriber = new ProbeTimeoutSubscriber<>(directSubscriber);

        probeTimeout.subscribe(probeTimeoutSubscriber);
        indirectDelay.subscribe(indirectDelaySubscriber);

        source.subscribe(directSubscriber);
    }

    static final class ProbeTimeoutSubscriber<P> implements CoreSubscriber<P> {

        final DirectSubscriber<?,?> directSubscriber;
        boolean once;

        ProbeTimeoutSubscriber(DirectSubscriber<?,?> directSubscriber) {
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
            directSubscriber.timeout();
        }
    }

    static final class IndirectDelaySubscriber<I> implements CoreSubscriber<I> {

        final DirectSubscriber<?,?> directSubscriber;
        boolean once;

        IndirectDelaySubscriber(DirectSubscriber<?,?> directSubscriber) {
            this.directSubscriber = directSubscriber;
        }

        @Override
        public void onSubscribe(Subscription s) {
            directSubscriber.setIndirectDelay(s);
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

    static final class IndirectSubscriber<T, C extends ProbeOperatorResult<? super T>> implements CoreSubscriber<T> {

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
            LOGGER.debug("IndirectSubscriber onNext {}", t);
            result.addIndirect(t);
        }

        @Override
        public void onError(Throwable t) {
            LOGGER.info("IndirectSubscriber onError {}", t);
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

    static final class DirectSubscriber<T, C extends ProbeOperatorResult<? super T>> implements CoreSubscriber<T>, Subscription {

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
            //indirect.cancel();
        }

        @Override
        public void onNext(T t) {
            LOGGER.debug("DirectSubscriber onNext {}", t);
            result.addDirect(t);
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
            // TODO call indirectSubscriber if error before RRT (indirectStart)
            //cancel();

            //actual.onError(t);
        }

        @Override
        public void onComplete() {
            LOGGER.debug("DirectSubscriber onComplete");
            directCompleted.compareAndSet(false, true);
        }

        void subscribeIndirect() {
            if (directCompleted.compareAndSet(false, true)) {
                result.indirect();
                indirectSubscriber.subscribe();
            }
        }

        void setIndirectDelay(Subscription s) {
            if (!INDIRECT.compareAndSet(this, null, s)) {
                s.cancel();
                if (indirect != Operators.cancelledSubscription()) {
                    Operators.reportSubscriptionSet();
                }
            }
        }

        void timeout() {
            actual.onNext(result);

            cancel();

            actual.onComplete();
        }

        void setProtocolPeriodEnd(Subscription s) {

        }
    }
}
