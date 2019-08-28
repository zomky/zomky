package io.github.zomky.gossip;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

// not thread-safe
public class SuspectTimers {

    private static final Logger LOGGER = LoggerFactory.getLogger(SuspectTimers.class);

    private int nodeId;
    private int suspicionMinMultiplier;
    private int suspicionMaxMultiplier;
    private int k;
    private Map<Integer, SuspectTimer> suspectTimers = new ConcurrentHashMap<>();
    private Map<Integer, Disposable> suspectDisposables = new ConcurrentHashMap<>();
    private DirectProcessor<Integer> deadNodesProcessor;
    private FluxSink<Integer> deadNodesSink;

    // TODO provide as parameter to builder
    private Scheduler scheduler = Schedulers.newSingle("suspect-timers", true);

    Flux<Integer> deadNodes() {
        return deadNodesProcessor;
    }

    void initializeTimer(int nodeId, Duration probeInterval, int clusterSize) {
        cancelTaskIfScheduled(nodeId);

        SuspectTimer suspectTimer = new SuspectTimer();
        suspectTimer.setCreated(System.currentTimeMillis());
        suspectTimer.setIndependentSuspicions(0);
        suspectTimer.setProbeInterval(probeInterval);
        suspectTimer.setClusterSize(clusterSize);
        suspectTimers.put(nodeId, suspectTimer);

        long suspicionTimeout = suspectTimer.suspicionTimeout(suspicionMinMultiplier, suspicionMaxMultiplier, k);
        scheduleTask(nodeId, suspicionTimeout, 0);
    }

    void incrementIndependentSuspicion(int nodeId) {
        cancelTaskIfScheduled(nodeId);

        SuspectTimer suspectTimer = suspectTimers.get(nodeId);
        if (suspectTimer != null) {
            int independentSuspicion = suspectTimer.incrementIndependentSuspicion();

            long suspicionTimeout = suspectTimer.suspicionTimeout(suspicionMinMultiplier, suspicionMaxMultiplier, k);
            long elapsed = suspectTimer.elapsed();
            if (elapsed >= suspicionTimeout) {
                deadNodesSink.next(nodeId);
            } else {
                scheduleTask(nodeId, suspicionTimeout - elapsed, independentSuspicion);
            }
        }
    }

    void removeTimer(int nodeId) {
        suspectTimers.remove(nodeId);
        cancelTaskIfScheduled(nodeId);
    }

    private void scheduleTask(int nodeId, long suspicionTimeout, int independentSuspicion) {
        LOGGER.info("[Node {}] Suspect node {} [suspicion: {}][current timeout: {}ms]", this.nodeId, nodeId, independentSuspicion, suspicionTimeout);
        final Disposable disposable = scheduler.schedule(() -> {
            suspectDisposables.remove(nodeId);
            deadNodesSink.next(nodeId);
        }, suspicionTimeout, TimeUnit.MILLISECONDS);

        suspectDisposables.put(nodeId, disposable);
    }

    private void cancelTaskIfScheduled(int nodeId) {
        Disposable currentScheduledFuture = suspectDisposables.get(nodeId);
        if (currentScheduledFuture != null && !currentScheduledFuture.isDisposed()) {
            currentScheduledFuture.dispose();
        }
    }

    private SuspectTimers() {}

    public static SuspectTimers.Builder builder() {
        return new SuspectTimers.Builder();
    }

    public static class Builder {
        private int nodeId;
        private int suspicionMinMultiplier = 3;
        private int suspicionMaxMultiplier = 2;
        private int k = 3;

        private Builder() {
        }

        public SuspectTimers.Builder nodeId(int nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public SuspectTimers.Builder suspicionMinMultiplier(int suspicionMinMultiplier) {
            this.suspicionMinMultiplier = suspicionMinMultiplier;
            return this;
        }

        public SuspectTimers.Builder suspicionMaxMultiplier(int suspicionMaxMultiplier) {
            this.suspicionMaxMultiplier = suspicionMaxMultiplier;
            return this;
        }

        public SuspectTimers.Builder k(int k) {
            this.k = k;
            return this;
        }

        public SuspectTimers build() {
            SuspectTimers suspectTimers = new SuspectTimers();
            suspectTimers.nodeId = nodeId;
            suspectTimers.suspicionMinMultiplier = suspicionMinMultiplier;
            suspectTimers.suspicionMaxMultiplier = suspicionMaxMultiplier;
            suspectTimers.k = k;

            suspectTimers.deadNodesProcessor = DirectProcessor.create();
            suspectTimers.deadNodesSink = suspectTimers.deadNodesProcessor.sink();

            return suspectTimers;
        }

    }

}
