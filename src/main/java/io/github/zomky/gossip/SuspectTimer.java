package io.github.zomky.gossip;

import com.google.common.math.DoubleMath;

import java.time.Duration;

class SuspectTimer {

    private long created;
    private volatile int independentSuspicions;
    private int clusterSize;
    private Duration probeInterval;

    void incrementIndependentSuspicion() {
        independentSuspicions++;
    }

    long suspicionTimeout(int suspicionMinMultiplier, int suspicionMaxMultiplier, int k) {
        long min = Math.round(suspicionMinMultiplier * Math.log10(clusterSize) * probeInterval.toMillis());
        long max = suspicionMaxMultiplier * min; // guava
        return Math.round(Math.max(min, max - (max - min) * DoubleMath.log2(independentSuspicions + 1) / DoubleMath.log2(k + 1)));
    }

    long elapsed() {
        return System.currentTimeMillis() - created;
    }

    public void setCreated(long created) {
        this.created = created;
    }

    public void setIndependentSuspicions(int independentSuspicions) {
        this.independentSuspicions = independentSuspicions;
    }

    public void setClusterSize(int clusterSize) {
        this.clusterSize = clusterSize;
    }

    public void setProbeInterval(Duration probeInterval) {
        this.probeInterval = probeInterval;
    }

}
