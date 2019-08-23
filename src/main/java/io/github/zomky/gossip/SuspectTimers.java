package io.github.zomky.gossip;

import com.google.common.math.DoubleMath;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SuspectTimers {

    private int suspicionMinMultiplier;
    private int suspicionMaxMultiplier;
    private int k;
    private Map<Integer, SuspectTimer> suspectTimers = new ConcurrentHashMap<>();

    void initializeTimer(int nodeId) {
        suspectTimers.put(nodeId, new SuspectTimer(System.currentTimeMillis(), 0));
    }

    void removeTimer(int nodeId) {
        suspectTimers.remove(nodeId);
    }

    List<Integer> removeTimers(Duration probeInterval, int clusterSize) {
        List<Integer> nodeIds = new ArrayList<>();
        suspectTimers.forEach((nodeId,suspectTimer) -> {
            if (suspectTimer.isOverdue(suspicionMinMultiplier, suspicionMaxMultiplier, k, clusterSize, probeInterval)) {
                nodeIds.add(nodeId);
            }
        });
        nodeIds.forEach(suspectTimers::remove);
        return nodeIds;
    }

    void incrementIndependentSuspicion(int nodeId) {
        SuspectTimer suspectTimer = suspectTimers.get(nodeId);
        if (suspectTimer != null) {
            suspectTimers.put(nodeId, suspectTimer.incrementIndependentSuspicion());
        }
    }

    static class SuspectTimer {

        private long created;
        private int independentSuspicions;

        SuspectTimer(long created, int independentSuspicions) {
            this.created = created;
            this.independentSuspicions = independentSuspicions;
        }

        SuspectTimer incrementIndependentSuspicion() {
            return new SuspectTimer(created, independentSuspicions + 1);
        }

        boolean isOverdue(int suspicionMinMultiplier, int suspicionMaxMultiplier, int k, int clusterSize, Duration probeInterval) {
            long suspicionTimeout = suspicionTimeout(suspicionMinMultiplier, suspicionMaxMultiplier, k, clusterSize, probeInterval);
            return System.currentTimeMillis() <= created + suspicionTimeout;
        }

        long suspicionTimeout(int suspicionMinMultiplier, int suspicionMaxMultiplier, int k, int clusterSize, Duration probeInterval) {
            long min = Math.round(suspicionMinMultiplier * Math.log10(clusterSize) * probeInterval.toMillis());
            long max = suspicionMaxMultiplier * min; // guava
            return Math.round(Math.max(min, max - (max - min) * DoubleMath.log2(independentSuspicions + 1) / DoubleMath.log2(k + 1)));
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
            return suspectTimers;
        }

    }

}
