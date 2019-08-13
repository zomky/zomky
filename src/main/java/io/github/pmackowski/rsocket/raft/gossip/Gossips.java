package io.github.pmackowski.rsocket.raft.gossip;

import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ack;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Gossip;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

class Gossips {

    private static final Logger LOGGER = LoggerFactory.getLogger(Gossips.class);

    private int nodeId;
    private int maxGossips;
    private int incarnation;
    private Map<Integer, GossipShared> gossips = new ConcurrentHashMap<>();

    void probeCompleted(ProbeResult probeResult) {
        makeGossipsLessHot(probeResult.getHotGossips());
        probeResult.getAcks().forEach(this::addAck);

        Gossip.Suspicion suspicion = probeResult.hasAck() ? Gossip.Suspicion.ALIVE : Gossip.Suspicion.SUSPECT;
        GossipShared gossipShared = gossips.get(probeResult.getDestinationNodeId());
        int incarnation = gossipShared == null ? 0 : gossipShared.getGossip().getIncarnation();
        addGossip(Gossip.newBuilder()
                .setNodeId(probeResult.getDestinationNodeId())
                .setSuspicion(suspicion)
                .setIncarnation(incarnation)
                .build()
        );
    }

    Ack onPing(int nodeId, int maxGossipShared, Ping ping) {
        List<Gossip> gossipsList = ping.getGossipsList();
        gossipsList.forEach(this::addGossip);
        List<Gossip> hotGossips = chooseHotGossips(maxGossipShared, gossipsList);
        makeGossipsLessHot(hotGossips);

        return Ack.newBuilder()
                .setNodeId(nodeId)
                .addAllGossips(hotGossips)
                .build();
    }

    // visible for testing
    void makeGossipsLessHot(List<Gossip> hotGossips) {
        hotGossips.forEach(gossip -> {
            int gossipShared = Optional
                .ofNullable(gossips.get(gossip.getNodeId()))
                .map(GossipShared::getShared)
                .orElse(0);

            gossips.put(gossip.getNodeId(), new GossipShared(gossip, gossipShared + 1));
        });
    }

    void addAck(Ack ack) {
        ack.getGossipsList().forEach(this::addGossip);
    }

    List<Gossip> chooseHotGossips() {
        return chooseHotGossips(Integer.MAX_VALUE);
    }

    List<Gossip> chooseHotGossips(int maxGossipShared) {
        return chooseHotGossips(maxGossipShared, new ArrayList<>());
    }

    List<Gossip> chooseHotGossips(List<Gossip> ignoreGossips) {
        return chooseHotGossips(Integer.MAX_VALUE, ignoreGossips);
    }

    List<Gossip> chooseHotGossips(int maxGossipShared, List<Gossip> ignoreGossips) {
        Set<Gossip> filterOut = new HashSet<>(ignoreGossips);
        return gossips.values()
                .stream()
                .filter(gossipShared -> gossipShared.getShared() < maxGossipShared)
                .filter(gossipShared -> !filterOut.contains(gossipShared.gossip))
                .sorted(Comparator.comparingInt(GossipShared::getShared))
                .map(GossipShared::getGossip)
                .limit(maxGossips)
                .collect(Collectors.toList());
    }

    Optional<Gossip> getGossip(int nodeId) {
        return Optional.ofNullable(gossips.get(nodeId)).map(GossipShared::getGossip);
    }

    int getGossipShared(int nodeId) {
        return Optional.ofNullable(gossips.get(nodeId))
                    .map(GossipShared::getShared)
                    .orElse(0);
    }

    int count() {
        return gossips.size();
    }

    int incarnation() {
        return incarnation;
    }

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
                } else {
                    LOGGER.info("[Node {}] Ignoring suspicion about itself due to stale incarnation number", this.nodeId);
                }
            }
            return;
        }

        GossipShared gossipShared = gossips.get(nodeId);
        if (gossipShared == null) {
            addGossipInternal(gossip(nodeId, incarnation, suspicion));
            return;
        }
        Gossip currentGossip = gossipShared.getGossip();
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
        gossips.put(gossip.getNodeId(), new GossipShared(gossip,0));
    }

    private static class GossipShared {

        private Gossip gossip;
        private int shared;

        public GossipShared(Gossip gossip, int shared) {
            this.gossip = gossip;
            this.shared = shared;
        }

        public Gossip getGossip() {
            return gossip;
        }

        public int getShared() {
            return shared;
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
        private List<GossipShared> gossipsShared = new ArrayList<>();

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

        public Gossips.Builder addGossip(Gossip gossip) {
            return addGossip(gossip, 0);
        }

        public Gossips.Builder addGossip(Gossip gossip, int shared) {
            this.gossipsShared.add(new GossipShared(gossip, shared));
            return this;
        }

        public Gossips build() {
            Gossips gossips = new Gossips();
            gossips.nodeId = nodeId;
            gossips.maxGossips = maxGossips;
            gossips.incarnation = incarnation;
            gossipsShared.forEach(gossipShared -> {
                Gossip gossip = gossipShared.getGossip();
                LOGGER.info("[Node {}] Initialize gossip [{} is {}, inc: {}]", this.nodeId, gossip.getNodeId(), gossip.getSuspicion(), gossip.getIncarnation());
                gossips.gossips.put(gossip.getNodeId(), gossipShared);
            });
            return gossips;
        }
    }

}
