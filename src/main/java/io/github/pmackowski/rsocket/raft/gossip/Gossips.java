package io.github.pmackowski.rsocket.raft.gossip;

import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ack;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Gossip;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

class Gossips {

    private static final Logger LOGGER = LoggerFactory.getLogger(Gossips.class);

    private int nodeId;
    private int maxGossips;
    private volatile int incarnation;
    private float gossipDisseminationMultiplier;
    private LocalHealthMultiplier localHealthMultiplier;
    private Map<Integer, GossipDissemination> gossips = new ConcurrentHashMap<>();

    synchronized void probeCompleted(ProbeResult probeResult) {
        makeGossipsLessHot(probeResult.getHotGossips());
        probeResult.getDistinctAcks().forEach(this::addAck);

        Gossip.Suspicion suspicion = probeResult.hasAck() ? Gossip.Suspicion.ALIVE : Gossip.Suspicion.SUSPECT;
        GossipDissemination gossipDissemination = gossips.get(probeResult.getDestinationNodeId());
        int incarnation = gossipDissemination == null ? 0 : gossipDissemination.getGossip().getIncarnation();
        addGossip(Gossip.newBuilder()
                .setNodeId(probeResult.getDestinationNodeId())
                .setSuspicion(suspicion)
                .setIncarnation(incarnation)
                .build()
        );
    }

    synchronized Ack onPing(int nodeId, int noPeers, Ping ping) {
        List<Gossip> gossipsList = ping.getGossipsList();
        gossipsList.forEach(this::addGossip);
        List<Gossip> hotGossips = chooseHotGossips(noPeers, gossipsList);
        makeGossipsLessHot(hotGossips);

        return Ack.newBuilder()
                .setNodeId(nodeId)
                .addAllGossips(hotGossips)
                .build();
    }

    synchronized void addAck(Ack ack) {
        ack.getGossipsList().forEach(this::addGossip);
    }

    synchronized void markDead(Duration suspicionTimeout) {
        gossips.values().stream()
                .filter(gossipDissemination -> gossipDissemination.getGossip().getSuspicion() == Gossip.Suspicion.SUSPECT)
                .forEach(gossipDissemination -> {

                });
    }

    void updateLocalHealthMultiplier(ProbeResult probeResult) {
        if (probeResult.hasAck()) {
            localHealthMultiplier.dec();
        } else {
            localHealthMultiplier.inc();
        }
        if (probeResult.hasMissedNack()) {
            localHealthMultiplier.inc();
        }
    }

    int localHealthMultiplier() {
        return localHealthMultiplier.value();
    }

    // visible for testing
    void makeGossipsLessHot(List<Gossip> hotGossips) {
        hotGossips.forEach(gossip -> {
            int disseminatedCount = Optional
                .ofNullable(gossips.get(gossip.getNodeId()))
                .map(GossipDissemination::getDisseminatedCount)
                .orElse(0);

            gossips.put(gossip.getNodeId(), new GossipDissemination(gossip, disseminatedCount + 1));
        });
    }

    List<Gossip> chooseHotGossips(int noPeers) {
        return chooseHotGossips(noPeers, new ArrayList<>());
    }

    List<Gossip> chooseHotGossips(int noPeers, List<Gossip> ignoreGossips) {
        Set<Gossip> filterOut = new HashSet<>(ignoreGossips);
        int maxGossipDissemination = maxGossipDissemination(noPeers);
        return gossips.values()
                .stream()
                .filter(gossipDissemination -> gossipDissemination.getDisseminatedCount() < maxGossipDissemination)
                .filter(gossipDissemination -> !filterOut.contains(gossipDissemination.gossip))
                .sorted(Comparator.comparingInt(GossipDissemination::getDisseminatedCount))
                .map(GossipDissemination::getGossip)
                .limit(maxGossips)
                .collect(Collectors.toList());
    }

    // visible for testing
    int maxGossipDissemination(int noPeers) {
        if (noPeers == 0) {
            return 0;
        }
        int log2Ceiling = Long.SIZE - Long.numberOfLeadingZeros(noPeers-1);
        return Math.round(gossipDisseminationMultiplier * (log2Ceiling+1));
    }

    Optional<Gossip> getGossip(int nodeId) {
        return Optional.ofNullable(gossips.get(nodeId)).map(GossipDissemination::getGossip);
    }

    int getDisseminatedCount(int nodeId) {
        return Optional.ofNullable(gossips.get(nodeId))
                    .map(GossipDissemination::getDisseminatedCount)
                    .orElse(0);
    }

    int count() {
        return gossips.size();
    }

    int incarnation() {
        return incarnation;
    }

    // visible for testing
    void addGossip(Gossip gossip) {
        int nodeId = gossip.getNodeId();
        int incarnation = gossip.getIncarnation();
        Gossip.Suspicion suspicion = gossip.getSuspicion();

        // Node gets gossip about itself
        if (this.nodeId == nodeId) {
            if (suspicion == Gossip.Suspicion.DEAD) {
                LOGGER.warn("[Node {}] I am dead and will be disconnected from cluster!", this.nodeId);
                addGossipInternal(gossip(nodeId, incarnation, Gossip.Suspicion.DEAD));
                // TODO definitely it is not everything
            } else if (suspicion == Gossip.Suspicion.SUSPECT) {
                if (incarnation == this.incarnation) {
                    LOGGER.info("[Node {}] I am suspected!", this.nodeId);
                    this.incarnation++;
                    addGossipInternal(gossip(this.nodeId, this.incarnation, Gossip.Suspicion.ALIVE));
                    localHealthMultiplier.inc();
                } else {
                    LOGGER.info("[Node {}] Ignoring suspicion about itself due to stale incarnation number", this.nodeId);
                }
            }
            return;
        }

        GossipDissemination gossipDissemination = gossips.get(nodeId);
        if (gossipDissemination == null) {
            addGossipInternal(gossip(nodeId, incarnation, suspicion));
            return;
        }
        Gossip currentGossip = gossipDissemination.getGossip();
        if (suspicion == Gossip.Suspicion.ALIVE) {
            if (incarnation > currentGossip.getIncarnation() && (currentGossip.getSuspicion() == Gossip.Suspicion.ALIVE || currentGossip.getSuspicion() == Gossip.Suspicion.SUSPECT)) {
                addGossipInternal(gossip(nodeId, incarnation, suspicion));
            }
            return;
        }

        if (suspicion == Gossip.Suspicion.SUSPECT) {
            if ((incarnation > currentGossip.getIncarnation() && currentGossip.getSuspicion() == Gossip.Suspicion.SUSPECT) ||
                    (incarnation >= currentGossip.getIncarnation() && currentGossip.getSuspicion() == Gossip.Suspicion.ALIVE)) {
                addGossipInternal(gossip(nodeId, incarnation, suspicion));
            }
            return;
        }

        if (suspicion == Gossip.Suspicion.DEAD) {
            addGossipInternal(gossip(nodeId, incarnation, suspicion));
        }
    }

    private Gossip gossip(int nodeId, int incarnation, Gossip.Suspicion suspicion) {
        return Gossip.newBuilder().setNodeId(nodeId).setSuspicion(suspicion).setIncarnation(incarnation).build();
    }

    private void addGossipInternal(Gossip gossip) {
        LOGGER.info("[Node {}] New gossip [{} is {}, inc: {}]", this.nodeId, gossip.getNodeId(), gossip.getSuspicion(), gossip.getIncarnation());
        gossips.put(gossip.getNodeId(), new GossipDissemination(gossip,0));
    }

    private static class GossipDissemination {

        private Gossip gossip;
        private int disseminatedCount;
        private long created;

        GossipDissemination(Gossip gossip, int disseminatedCount) {
            this.gossip = gossip;
            this.disseminatedCount = disseminatedCount;
            this.created = System.currentTimeMillis();
        }

        public Gossip getGossip() {
            return gossip;
        }

        int getDisseminatedCount() {
            return disseminatedCount;
        }
    }

    private Gossips() {}

    public static Gossips.Builder builder() {
        return new Gossips.Builder();
    }

    public static class Builder {

        private int nodeId;
        private int maxGossips = Integer.MAX_VALUE;
        private int incarnation;
        private float gossipDisseminationMultiplier = 1f;
        private int maxLocalHealthMultiplier = 8;
        private List<GossipDissemination> gossipDisseminations = new ArrayList<>();

        private Builder() {
        }

        public Gossips.Builder nodeId(int nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Gossips.Builder maxGossips(int maxGossips) {
            this.maxGossips = maxGossips;
            return this;
        }

        public Gossips.Builder incarnation(int incarnation) {
            this.incarnation = incarnation;
            return this;
        }

        public Gossips.Builder gossipDisseminationMultiplier(float gossipDisseminationMultiplier) {
            this.gossipDisseminationMultiplier = gossipDisseminationMultiplier;
            return this;
        }

        public Gossips.Builder maxLocalHealthMultiplier(int maxLocalHealthMultiplier) {
            this.maxLocalHealthMultiplier = maxLocalHealthMultiplier;
            return this;
        }

        public Gossips.Builder addGossip(Gossip gossip) {
            return addGossip(gossip, 0);
        }

        public Gossips.Builder addGossip(Gossip gossip, int gossipDisseminatedCount) {
            this.gossipDisseminations.add(new GossipDissemination(gossip, gossipDisseminatedCount));
            return this;
        }

        public Gossips build() {
            Gossips gossips = new Gossips();
            gossips.nodeId = nodeId;
            gossips.maxGossips = maxGossips;
            gossips.incarnation = incarnation;
            gossips.gossipDisseminationMultiplier = gossipDisseminationMultiplier;
            gossips.localHealthMultiplier = new LocalHealthMultiplier(maxLocalHealthMultiplier);
            gossipDisseminations.forEach(gossipDissemination -> {
                Gossip gossip = gossipDissemination.getGossip();
                LOGGER.info("[Node {}] Initialize gossip [{} is {}, inc: {}]", this.nodeId, gossip.getNodeId(), gossip.getSuspicion(), gossip.getIncarnation());
                gossips.gossips.put(gossip.getNodeId(), gossipDissemination);
            });
            return gossips;
        }
    }

}
