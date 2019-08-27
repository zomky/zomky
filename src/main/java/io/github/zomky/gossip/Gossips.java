package io.github.zomky.gossip;

import com.google.common.collect.*;
import io.github.zomky.gossip.protobuf.Ack;
import io.github.zomky.gossip.protobuf.Gossip;
import io.github.zomky.gossip.protobuf.Ping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.ReplayProcessor;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

class Gossips {

    private static final Logger LOGGER = LoggerFactory.getLogger(Gossips.class);

    private int nodeId;
    private int maxGossips;
    private volatile int incarnation;
    private Duration baseProbeInterval;
    private float gossipDisseminationMultiplier;
    private LocalHealthMultiplier localHealthMultiplier;
    private Map<Integer, GossipDissemination> aliveGossips = new ConcurrentHashMap<>();
    private Map<Integer, GossipDissemination> deadGossips = new ConcurrentHashMap<>();
    // row - nodeId, column - nodeIdHarbourSuspicion
    // guava
    private Table<Integer, Integer, GossipDissemination> suspectGossips = HashBasedTable.create();
    private SuspectTimers suspectTimers = SuspectTimers.builder().build();

    private ReplayProcessor<Gossip> processor;
    private FluxSink<Gossip> sink;
    private Disposable deadNodesDisposable;

    synchronized void probeCompleted(ProbeResult probeResult) {
        if (probeResult.hasAck()) {
            makeGossipsLessHot(probeResult.getHotGossips());
        }
        probeResult.getDistinctAcks().forEach(this::addAck);

        if (probeResult.hasAck()) {
            addGossip(Gossip.newBuilder()
                .setNodeId(probeResult.getDestinationNodeId())
                .setIncarnation(currentIncarnation(probeResult.getDestinationNodeId()))
                .setSuspicion(Gossip.Suspicion.ALIVE)
                .build()
            );
        } else {
            addGossip(Gossip.newBuilder()
                .setNodeId(probeResult.getDestinationNodeId())
                .setNodeIdHarbourSuspicion(nodeId)
                .setIncarnation(currentIncarnation(probeResult.getDestinationNodeId()))
                .setSuspicion(Gossip.Suspicion.SUSPECT)
                .build()
            );
        }
    }

    synchronized Ack onPing(int nodeId, Ping ping) {
        return onPing(nodeId, ping, false);
    }

    synchronized Ack onPing(int nodeId, Ping ping, boolean tcp) {
        List<Gossip> gossipsList = ping.getGossipsList();
        gossipsList.forEach(this::addGossipIgnoreError);
        List<Gossip> hotGossips = chooseHotGossips(gossipsList);
        makeGossipsLessHot(hotGossips);

        return Ack.newBuilder()
                .setNodeId(nodeId)
                .addAllGossips(hotGossips)
                .setTcp(tcp)
                .build();
    }

    synchronized void addAck(Ack ack) {
        ack.getGossipsList().forEach(this::addGossipIgnoreError);
    }

    synchronized void addGossips(List<Gossip> gossips) {
        gossips.forEach(this::addGossipInternal);
    }

    Flux<Gossip> peerChanges() {
        return processor.filter(gossip -> gossip.getNodeId() != nodeId);
    }

    Duration probeInterval() {
        return Duration.ofMillis(baseProbeInterval.toMillis() * (localHealthMultiplier() + 1));
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
            getGossipDissemination(gossip.getNodeId(), gossip.getNodeIdHarbourSuspicion()).ifPresent(
                gossipDissemination -> addGossipInternal(gossip, gossipDissemination.getDisseminatedCount() + 1)
            );
        });
    }

    List<Gossip> allGossips() { // TODO inc dissemination ?
        return allGossipDisseminations().stream().map(GossipDissemination::getGossip).collect(Collectors.toList());
    }

    List<Gossip> chooseHotGossips(int destinationNodeId) {
        Comparator<GossipDissemination> buddy = Comparator.comparingInt(gossipDissemination -> gossipDissemination.isBuddy(destinationNodeId) ? 0 : 1);
        int maxGossipDissemination = maxGossipDissemination();
        return allGossipDisseminations()
                .stream()
                .filter(gossipDissemination -> gossipDissemination.getDisseminatedCount() < maxGossipDissemination || gossipDissemination.isBuddy(destinationNodeId))
                .sorted(buddy.thenComparingInt(GossipDissemination::getDisseminatedCount))
                .map(GossipDissemination::getGossip)
                .limit(maxGossips)
                .collect(Collectors.toList());
    }

    List<Gossip> chooseHotGossips(List<Gossip> ignoreGossips) {
        Set<Gossip> filterOut = new HashSet<>(ignoreGossips);
        int maxGossipDissemination = maxGossipDissemination();
        return allGossipDisseminations()
                .stream()
                .filter(gossipDissemination -> gossipDissemination.getDisseminatedCount() < maxGossipDissemination)
                .filter(gossipDissemination -> !filterOut.contains(gossipDissemination.gossip))
                .sorted(Comparator.comparingInt(GossipDissemination::getDisseminatedCount))
                .map(GossipDissemination::getGossip)
                .limit(maxGossips)
                .collect(Collectors.toList());
    }

    // visible for testing
    int maxGossipDissemination() {
        return maxGossipDissemination(estimatedClusterSize());
    }

    int maxGossipDissemination(int gossipsCount) {
        if (gossipsCount == 0) {
            return 0;
        }
        int log2Ceiling = Long.SIZE - Long.numberOfLeadingZeros(gossipsCount-1);
        return Math.round(gossipDisseminationMultiplier * (log2Ceiling+1));
    }

    int estimatedClusterSize() {
        return aliveGossips.size() +
                suspectGossips.rowKeySet().size() +
                (noGossips(nodeId) ? 1 : 0);
    }

    int incarnation() {
        return incarnation;
    }

    private void addGossipIgnoreError(Gossip gossip) {
        try {
            addGossip(gossip);
        } catch (Exception e) {
            LOGGER.warn(String.format("[Node %s] Gossip %s has been added to following error", nodeId, gossip), e);
        }
    }

    // visible for testing
    void addGossip(Gossip gossip) {
        int nodeId = gossip.getNodeId();
        int nodeIdHarbourSuspicion = gossip.getNodeIdHarbourSuspicion();
        int incarnation = gossip.getIncarnation();
        Gossip.Suspicion suspicion = gossip.getSuspicion();

        if (suspicion == Gossip.Suspicion.SUSPECT && nodeIdHarbourSuspicion == 0) {
            throw new GossipException(String.format("Harbour suspicion must be defined for gossip %s!", gossip));
        }
        if (suspicion != Gossip.Suspicion.SUSPECT && nodeIdHarbourSuspicion != 0) {
            throw new GossipException(String.format("Harbour suspicion must not be defined for gossip %s!", gossip));
        }

        // Node gets gossip about itself
        if (this.nodeId == nodeId) {
            if (suspicion == Gossip.Suspicion.DEAD) {
                LOGGER.warn("[Node {}] I am dead and will be disconnected from cluster!", this.nodeId);
                addDeadGossipInternal(nodeId, incarnation);
                // TODO definitely it is not everything
            } else if (suspicion == Gossip.Suspicion.SUSPECT) {
                if (incarnation == this.incarnation) {
                    LOGGER.info("[Node {}] I am suspected!", this.nodeId);
                    this.incarnation++;
                    addAliveGossipInternal(nodeId, this.incarnation);
                    localHealthMultiplier.inc();
                } else {
                    LOGGER.info("[Node {}] Ignoring suspicion about itself due to stale incarnation number", this.nodeId);
                }
            }
            return;
        }

        if (noGossips(nodeId)) {
            addGossipInternal(gossip);
            return;
        }

        int currentIncarnation = currentIncarnation(nodeId);
        Gossip.Suspicion currentSuspicion = currentSuspicion(nodeId);

        if (suspicion == Gossip.Suspicion.ALIVE) {
            if (incarnation > currentIncarnation && (currentSuspicion == Gossip.Suspicion.ALIVE || currentSuspicion == Gossip.Suspicion.SUSPECT)) {
                addGossipInternal(gossip);
            }
            return;
        }

        if (suspicion == Gossip.Suspicion.SUSPECT) {
            if (currentSuspicion == Gossip.Suspicion.ALIVE) {
                if (incarnation >= currentIncarnation) {
                    addGossipInternal(gossip);
                }
                return;
            }
            if (currentSuspicion == Gossip.Suspicion.SUSPECT) {
                if (incarnation > currentIncarnation ||
                        (incarnation == currentIncarnation && !getGossip(nodeId, nodeIdHarbourSuspicion).isPresent())) {
                    addGossipInternal(gossip);
                }
                return;
            }
        }

        if (suspicion == Gossip.Suspicion.DEAD && currentSuspicion != Gossip.Suspicion.DEAD) {
            addGossipInternal(gossip);
        }
    }

    private boolean noGossips(int nodeId) {
        return getAllGossipDisseminations(nodeId).isEmpty();
    }

    private void addGossipInternal(Gossip gossip) {
        addGossipInternal(gossip, 0);
    }

    private void addGossipInternal(Gossip gossip, int disseminatedCount) {
        if (gossip.getNodeIdHarbourSuspicion() == 0) {
            LOGGER.info("[Node {}] Gossip [{} is {}, inc: {}, disseminated: {}]", this.nodeId, gossip.getNodeId(), gossip.getSuspicion(), gossip.getIncarnation(), disseminatedCount);
        } else {
            LOGGER.info("[Node {}] Gossip [{} is {} by {}, inc: {}, disseminated: {}]", this.nodeId, gossip.getNodeId(), gossip.getSuspicion(), gossip.getNodeIdHarbourSuspicion(), gossip.getIncarnation(), disseminatedCount);
        }
        int nodeId = gossip.getNodeId();
        int nodeIdHarbourSuspicion = gossip.getNodeIdHarbourSuspicion();
        int incarnation = gossip.getIncarnation();
        Gossip.Suspicion suspicion = gossip.getSuspicion();

        Gossip.Suspicion currentSuspicion = currentSuspicion(nodeId);
        Gossip.Suspicion newSuspicion = gossip.getSuspicion();
        if (currentSuspicion != newSuspicion && gossip.getNodeId() != this.nodeId) {
            sink.next(gossip);
        }

        switch (suspicion) {
            case ALIVE:   addAliveGossipInternal(nodeId, incarnation, disseminatedCount); break;
            case SUSPECT: addSuspectGossipInternal(nodeId, nodeIdHarbourSuspicion, incarnation, disseminatedCount); break;
            case DEAD: addDeadGossipInternal(nodeId, incarnation, disseminatedCount); break;
        }
    }

    private void addAliveGossipInternal(int nodeId, int incarnation) {
        addAliveGossipInternal(nodeId, incarnation, 0);
    }

    private void addAliveGossipInternal(int nodeId, int incarnation, int disseminationCount) {
        Gossip gossip = Gossip.newBuilder()
                .setNodeId(nodeId)
                .setIncarnation(incarnation)
                .setSuspicion(Gossip.Suspicion.ALIVE)
                .build();
        aliveGossips.put(nodeId, new GossipDissemination(gossip, disseminationCount));
        deadGossips.remove(nodeId);
        suspectGossips.row(nodeId).clear();
        suspectTimers.removeTimer(nodeId);
    }

    private void addSuspectGossipInternal(int nodeId, int nodeIdHarbourSuspicion, int incarnation, int disseminationCount) {
        Gossip gossip = Gossip.newBuilder()
                .setNodeId(nodeId)
                .setNodeIdHarbourSuspicion(nodeIdHarbourSuspicion)
                .setIncarnation(incarnation)
                .setSuspicion(Gossip.Suspicion.SUSPECT)
                .build();
        int currentIncarnation = currentIncarnation(nodeId);
        if (incarnation > currentIncarnation) {
            suspectGossips.row(nodeId).clear();
            suspectTimers.removeTimer(nodeId);
        }
        if (!suspectGossips.containsRow(nodeId)) {
            suspectTimers.initializeTimer(nodeId, probeInterval(), estimatedClusterSize());
        } else {
            suspectTimers.incrementIndependentSuspicion(nodeId);
        }

        suspectGossips.put(nodeId, nodeIdHarbourSuspicion, new GossipDissemination(gossip, disseminationCount));
        aliveGossips.remove(nodeId);
    }

    private void addDeadGossipInternal(int nodeId, int incarnation) {
        addDeadGossipInternal(nodeId, incarnation, 0);
    }

    private void addDeadGossipInternal(int nodeId, int incarnation, int disseminationCount) {
        Gossip gossip = Gossip.newBuilder()
                .setNodeId(nodeId)
                .setIncarnation(incarnation)
                .setSuspicion(Gossip.Suspicion.DEAD)
                .build();
        deadGossips.put(nodeId, new GossipDissemination(gossip, disseminationCount));
        aliveGossips.remove(nodeId);
        suspectGossips.row(nodeId).clear();
        suspectTimers.removeTimer(nodeId);
    }

    private Optional<Gossip> getGossip(int nodeId, int nodeHarbourId) {
        return getGossipDissemination(nodeId, nodeHarbourId).map(GossipDissemination::getGossip);
    }

    int getDisseminatedCount(int nodeId) {
        return getDisseminatedCount(nodeId, 0);
    }

    int getDisseminatedCount(int nodeId, int nodeIdHarbourSuspicion) {
        return getGossipDissemination(nodeId, nodeIdHarbourSuspicion).map(GossipDissemination::getDisseminatedCount).orElse(0);
    }

    private Optional<GossipDissemination> getGossipDissemination(int nodeId, int nodeHarbourId) {
        GossipDissemination alive = aliveGossips.get(nodeId);
        if (alive != null) {
            return Optional.of(alive);
        }
        GossipDissemination dead = deadGossips.get(nodeId);
        if (dead != null) {
            return Optional.of(dead);
        }
        if (nodeHarbourId == 0) {
            return Optional.empty();
        }
        return Optional.ofNullable(suspectGossips.get(nodeId, nodeHarbourId));
    }

    private List<GossipDissemination> getAllGossipDisseminations(int nodeId) {
        GossipDissemination alive = aliveGossips.get(nodeId);
        if (alive != null) {
            return ImmutableList.of(alive);
        }
        GossipDissemination dead = deadGossips.get(nodeId);
        if (dead != null) {
            return ImmutableList.of(dead);
        }

        return new ArrayList<>(suspectGossips.row(nodeId).values());
    }

    private int currentIncarnation(int nodeId) {
        return getAllGossipDisseminations(nodeId).stream().mapToInt(v -> v.getGossip().getIncarnation()).max().orElse(0);
    }

    private Gossip.Suspicion currentSuspicion(int nodeId) {
        return getAllGossipDisseminations(nodeId).stream()
                .map(GossipDissemination::getGossip)
                .map(Gossip::getSuspicion)
                .findAny()
                .orElse(Gossip.Suspicion.UNKNOWN);
    }

    private List<GossipDissemination> allGossipDisseminations() {
        List<GossipDissemination> result = new ArrayList<>();
        result.addAll(aliveGossips.values());
        result.addAll(suspectGossips.values());
        result.addAll(deadGossips.values());
        return result;
    }

    private static class GossipDissemination {

        private Gossip gossip;
        private int disseminatedCount;

        GossipDissemination(Gossip gossip, int disseminatedCount) {
            this.gossip = gossip;
            this.disseminatedCount = disseminatedCount;
        }

        public Gossip getGossip() {
            return gossip;
        }

        int getDisseminatedCount() {
            return disseminatedCount;
        }

        boolean isBuddy(int nodeId) {
            return gossip.getNodeId() == nodeId && gossip.getSuspicion() == Gossip.Suspicion.SUSPECT;
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
        private Duration baseProbeInterval;
        private float gossipDisseminationMultiplier = 1f;
        private int maxLocalHealthMultiplier = 8;
        private boolean addAliveGossipAboutItself;
        private Map<Integer, GossipDissemination> aliveGossips = new ConcurrentHashMap<>();
        private Map<Integer, GossipDissemination> deadGossips = new ConcurrentHashMap<>();
        private Multimap<Integer, GossipDissemination> suspectGossips = ArrayListMultimap.create();
        private SuspectTimers suspectTimers = SuspectTimers.builder().build();

        private Builder() {
        }

        public Gossips.Builder nodeId(int nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Gossips.Builder incarnation(int incarnation) {
            this.incarnation = incarnation;
            return this;
        }

        public Gossips.Builder baseProbeInterval(Duration baseProbeInterval) {
            this.baseProbeInterval = baseProbeInterval;
            return this;
        }

        public Gossips.Builder maxGossips(int maxGossips) {
            this.maxGossips = maxGossips;
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

        public Gossips.Builder addAliveGossipAboutItself() {
            this.addAliveGossipAboutItself = true;
            return this;
        }

        public Gossips.Builder suspectTimers(SuspectTimers suspectTimers) {
            this.suspectTimers = suspectTimers;
            return this;
        }

        public Gossips.Builder addGossip(Gossip gossip, int disseminatedCount) {
            if (gossip.getSuspicion() == Gossip.Suspicion.ALIVE) {
                return addAliveGossip(gossip.getNodeId(), gossip.getIncarnation(), disseminatedCount);
            } else if (gossip.getSuspicion() == Gossip.Suspicion.SUSPECT) {
                return addSuspectGossip(gossip.getNodeId(), gossip.getNodeIdHarbourSuspicion(), gossip.getIncarnation(), disseminatedCount);
            } else if (gossip.getSuspicion() == Gossip.Suspicion.DEAD) {
                return addDeadGossip(gossip.getNodeId(), gossip.getIncarnation(), disseminatedCount);
            } else {
                throw new GossipException("Suspicion should be one of [ALIVE,SUSPECT,DEAD]");
            }
        }

        public Gossips.Builder addAliveGossip(int nodeId, int incarnation) {
            return addAliveGossip(nodeId, incarnation, 0);
        }

        public Gossips.Builder addAliveGossip(int nodeId, int incarnation, int disseminatedCount) {
            Gossip gossip = Gossip.newBuilder()
                    .setNodeId(nodeId)
                    .setIncarnation(incarnation)
                    .setSuspicion(Gossip.Suspicion.ALIVE)
                    .build();
            aliveGossips.put(nodeId, new GossipDissemination(gossip, disseminatedCount));
            return this;
        }

        public Gossips.Builder addSuspectGossip(int nodeId, int nodeIdHarbourSuspicion, int incarnation) {
            return addSuspectGossip(nodeId, nodeIdHarbourSuspicion, incarnation, 0);
        }

        public Gossips.Builder addSuspectGossip(int nodeId, int nodeIdHarbourSuspicion, int incarnation, int disseminatedCount) {
            Gossip gossip = Gossip.newBuilder()
                    .setNodeId(nodeId)
                    .setNodeIdHarbourSuspicion(nodeIdHarbourSuspicion)
                    .setIncarnation(incarnation)
                    .setSuspicion(Gossip.Suspicion.SUSPECT)
                    .build();
            suspectGossips.remove(nodeId, gossip);
            suspectGossips.put(nodeId, new GossipDissemination(gossip, disseminatedCount));
            return this;
        }

        public Gossips.Builder addDeadGossip(int nodeId, int incarnation, int disseminatedCount) {
            Gossip gossip = Gossip.newBuilder()
                    .setNodeId(nodeId)
                    .setIncarnation(incarnation)
                    .setSuspicion(Gossip.Suspicion.DEAD)
                    .build();
            deadGossips.put(nodeId, new GossipDissemination(gossip, disseminatedCount));
            return this;
        }

        public Gossips build() {
            Gossips gossips = new Gossips();
            gossips.nodeId = nodeId;
            gossips.maxGossips = maxGossips;
            gossips.incarnation = incarnation;
            gossips.baseProbeInterval = baseProbeInterval;
            gossips.gossipDisseminationMultiplier = gossipDisseminationMultiplier;
            gossips.localHealthMultiplier = new LocalHealthMultiplier(maxLocalHealthMultiplier);
            gossips.suspectTimers = suspectTimers;
            if (addAliveGossipAboutItself) {
                addAliveGossip(nodeId, 0);
            }
            aliveGossips.values().forEach(gossipDissemination -> {
                Gossip gossip = gossipDissemination.getGossip();
                LOGGER.info("[Node {}] Initialize gossip [{} is {}, inc: {}]", this.nodeId, gossip.getNodeId(), gossip.getSuspicion(), gossip.getIncarnation());
                gossips.aliveGossips.put(gossip.getNodeId(), gossipDissemination);
            });
            suspectGossips.values().forEach(gossipDissemination -> {
                Gossip gossip = gossipDissemination.getGossip();
                LOGGER.info("[Node {}] Initialize gossip [{} is {} by {}, inc: {}]", this.nodeId, gossip.getNodeId(), gossip.getSuspicion(), gossip.getNodeIdHarbourSuspicion(), gossip.getIncarnation());
                gossips.suspectGossips.put(gossip.getNodeId(), gossip.getNodeIdHarbourSuspicion(), gossipDissemination);
            });
            deadGossips.values().forEach(gossipDissemination -> {
                Gossip gossip = gossipDissemination.getGossip();
                LOGGER.info("[Node {}] Initialize gossip [{} is {}, inc: {}]", this.nodeId, gossip.getNodeId(), gossip.getSuspicion(), gossip.getIncarnation());
                gossips.deadGossips.put(gossip.getNodeId(), gossipDissemination);
            });
            gossips.processor = ReplayProcessor.createTimeout(Duration.ofHours(1));
            gossips.sink = gossips.processor.sink(FluxSink.OverflowStrategy.DROP);
            gossips.deadNodesDisposable = suspectTimers.deadNodes()
                    .doOnNext(nodeId -> addDeadGossip(nodeId, 0, 0)) // TODO make it thread-safe
                    .subscribe();
            return gossips;
        }
    }

}
