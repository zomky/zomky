package io.github.zomky.gossip;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;

public class SuspectTimers {

    private static final Logger LOGGER = LoggerFactory.getLogger(SuspectTimers.class);

    private int suspicionMinMultiplier;
    private int suspicionMaxMultiplier;
    private int k;
    private Map<Integer, SuspectTimer> suspectTimers = new ConcurrentHashMap<>();
    private Map<Integer, ScheduledFuture<Integer>> suspectFutures = new ConcurrentHashMap<>();
    private DirectProcessor<Integer> deadNodesProcessor;
    private FluxSink<Integer> deadNodesSink;

    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

    Flux<Integer> deadNodes() {
        return deadNodesProcessor;
    }

    void initializeTimer(int nodeId) {
    // TODO remove
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
        scheduleTask(nodeId, suspicionTimeout);
    }

    void incrementIndependentSuspicion(int nodeId) {
        cancelTaskIfScheduled(nodeId);

        SuspectTimer suspectTimer = suspectTimers.get(nodeId);
        if (suspectTimer != null) {
            suspectTimer.incrementIndependentSuspicion();

            long suspicionTimeout = suspectTimer.suspicionTimeout(suspicionMinMultiplier, suspicionMaxMultiplier, k);
            long elapsed = suspectTimer.elapsed();
            if (elapsed >= suspicionTimeout) {
                LOGGER.warn("Marking node {} as dead!", nodeId);
                deadNodesSink.next(nodeId);
            } else {
                scheduleTask(nodeId, suspicionTimeout - elapsed);
            }
        }
    }

    void removeTimer(int nodeId) {
        suspectTimers.remove(nodeId);
        cancelTaskIfScheduled(nodeId);
    }

    private void scheduleTask(int nodeId, long suspicionTimeout) {
        LOGGER.info("Suspecting node {} [timeout: {}ms]", nodeId, suspicionTimeout);
        ScheduledFuture<Integer> schedule = executorService.schedule(() -> {
            LOGGER.info("Suspecting node {} timeout reached.", nodeId);
            suspectFutures.remove(nodeId);
            deadNodesSink.next(nodeId);
            return nodeId;
        }, suspicionTimeout, TimeUnit.MILLISECONDS);
        suspectFutures.put(nodeId, schedule);
    }

    private void cancelTaskIfScheduled(int nodeId) {
        ScheduledFuture<Integer> currentScheduledFuture = suspectFutures.get(nodeId);
        if (currentScheduledFuture != null) {
            boolean cancelled = currentScheduledFuture.cancel(false);
            if (cancelled) {
                LOGGER.info("CANCELLED Cancelling timer for node {}", nodeId);
            } else {
                LOGGER.info("NOT CANCELLED Cancelling timer for node {}", nodeId);
            }
        }
    }

    private SuspectTimers() {}

    public static SuspectTimers.Builder builder() {
        return new SuspectTimers.Builder();
    }

    public static class Builder {
        private int suspicionMinMultiplier = 3;
        private int suspicionMaxMultiplier = 2;
        private int k = 3;

        private Builder() {
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
            suspectTimers.suspicionMinMultiplier = suspicionMinMultiplier;
            suspectTimers.suspicionMaxMultiplier = suspicionMaxMultiplier;
            suspectTimers.k = k;

            suspectTimers.deadNodesProcessor = DirectProcessor.create();
            suspectTimers.deadNodesSink = suspectTimers.deadNodesProcessor.sink();

            return suspectTimers;
        }

    }

}
