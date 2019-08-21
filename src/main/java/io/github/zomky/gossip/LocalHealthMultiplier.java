package io.github.zomky.gossip;

import java.util.concurrent.atomic.AtomicInteger;

class LocalHealthMultiplier {

    private int maxLocalHealthMultiplier;
    private AtomicInteger localHealthMultiplier = new AtomicInteger(0);

    LocalHealthMultiplier(int maxLocalHealthMultiplier) {
        this.maxLocalHealthMultiplier = maxLocalHealthMultiplier;
    }

    int inc() {
        return localHealthMultiplier.updateAndGet(n -> Math.min(n+1, maxLocalHealthMultiplier));
    }

    int dec() {
        return localHealthMultiplier.updateAndGet(n -> Math.max(n-1,0));
    }

    int value() {
        return localHealthMultiplier.intValue();
    }
}
