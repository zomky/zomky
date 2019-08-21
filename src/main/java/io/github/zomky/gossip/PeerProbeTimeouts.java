package io.github.zomky.gossip;

import java.time.Duration;

class PeerProbeTimeouts {

    private Duration baseProbeTimeout;
    private int localHealthMultiplier;
    private float nackRatio;
    private float indirectDelayRatio;

    Duration probeTimeout() {
        return Duration.ofMillis(baseProbeTimeout.toMillis() * (localHealthMultiplier + 1));
    }

    Duration indirectDelay() {
        return Duration.ofMillis(Math.round(probeTimeout().toMillis() * indirectDelayRatio));
    }

    Duration nackTimeout() {
        return Duration.ofMillis(Math.round((probeTimeout().toMillis() - indirectDelay().toMillis()) * nackRatio));
    }

    private PeerProbeTimeouts() {}

    public static PeerProbeTimeouts.Builder builder() {
        return new PeerProbeTimeouts.Builder();
    }

    public static class Builder {

        private Duration baseProbeTimeout;
        private int localHealthMultiplier;
        private float nackRatio;
        private float indirectDelayRatio;

        private Builder() {
        }

        public PeerProbeTimeouts.Builder baseProbeTimeout(Duration baseProbeTimeout) {
            this.baseProbeTimeout = baseProbeTimeout;
            return this;
        }

        public PeerProbeTimeouts.Builder localHealthMultiplier(int localHealthMultiplier) {
            this.localHealthMultiplier = localHealthMultiplier;
            return this;
        }

        public PeerProbeTimeouts.Builder nackRatio(float nackRatio) {
            this.nackRatio = nackRatio;
            return this;
        }

        public PeerProbeTimeouts.Builder indirectDelayRatio(float indirectDelayRatio) {
            this.indirectDelayRatio = indirectDelayRatio;
            return this;
        }

        public PeerProbeTimeouts build() {
            PeerProbeTimeouts peerProbeTimeouts = new PeerProbeTimeouts();
            peerProbeTimeouts.baseProbeTimeout = baseProbeTimeout;
            peerProbeTimeouts.localHealthMultiplier = localHealthMultiplier;
            peerProbeTimeouts.nackRatio = nackRatio;
            peerProbeTimeouts.indirectDelayRatio = indirectDelayRatio;
            return peerProbeTimeouts;
        }
    }

}
