package io.github.zomky.raft;

import io.github.zomky.Cluster;
import io.github.zomky.listener.ConfigurationChangeListener;
import io.github.zomky.listener.ConfirmListener;
import io.github.zomky.listener.LastAppliedListener;
import io.github.zomky.metrics.MetricsCollector;
import io.github.zomky.storage.InMemoryRaftStorage;
import io.github.zomky.storage.RaftStorage;
import io.github.zomky.storage.log.entry.CommandEntry;
import io.github.zomky.storage.log.entry.ConfigurationEntry;
import io.github.zomky.storage.log.entry.IndexedLogEntry;
import io.github.zomky.storage.log.entry.LogEntry;
import io.github.zomky.storage.log.reader.LogStorageReader;
import io.github.zomky.storage.meta.Configuration;
import io.github.zomky.transport.Sender;
import io.github.zomky.transport.protobuf.*;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RaftGroup {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftGroup.class);

    /**
     * index of highest log entry known to be
     * committed (initialized to 0, increases
     * monotonically)
     */
    private volatile long commitIndex;

    RaftStorage raftStorage;
    private MetricsCollector metrics;
    private Cluster cluster;
    private String groupName;
    private RaftConfiguration raftConfiguration;
    private LogStorageReader logStorageReader;

    private volatile RaftRole nodeState = new FollowerRole();
    private AtomicInteger currentLeaderId = new AtomicInteger(0);
    private AtomicLong lastApplied = new AtomicLong(0);
    private AtomicLong lastAppendEntriesCall = new AtomicLong(0); // maybe globally for all groups ?
    private volatile Duration currentElectionTimeout = Duration.ofMillis(0);

    private List<ConfirmListener> confirmListeners = new CopyOnWriteArrayList<>();
    private List<LastAppliedListener> lastAppliedListeners = new CopyOnWriteArrayList<>();

    private List<ConfigurationChangeListener> configurationChangeListeners = new CopyOnWriteArrayList<>();
    private volatile Configuration currentConfiguration;
    private volatile long previousConfigurationId;
    private volatile long currentConfigurationId;

    Lock appendLock = new ReentrantLock();
    Lock configurationLock = new ReentrantLock();
    DirectProcessor<Long> advanceStateMachineProcessor = DirectProcessor.create();
    FluxSink<Long> advanceStateMachineSink = advanceStateMachineProcessor.sink();
    Disposable advanceStateMachineDisposable;

    public void appendLock() {
        appendLock.lock();
    }

    public void appendUnlock() {
        appendLock.unlock();
    }

    public RaftConfiguration getRaftConfiguration() {
        return raftConfiguration;
    }

    void onInit() {
        advanceStateMachineDisposable = Flux.from(advanceStateMachineProcessor)
                .onBackpressureBuffer()
                .doOnNext(this::advanceStateMachine)
                .subscribe();

        nodeState.onInit(cluster, this, raftStorage);
    }

    public void onExit() {
        advanceStateMachineDisposable.dispose();
        advanceStateMachineSink.complete();
    }

    boolean hasClients() {
        return !lastAppliedListeners.isEmpty();
    }

    public RaftStorage getRaftStorage() {
        return raftStorage;
    }

    public boolean isPreVote() {
        return raftConfiguration.isPreVote();
    }

    public boolean isLeaderStickiness() {
        return raftConfiguration.isLeaderStickiness();
    }

    void voteForMyself() {
        int term = raftStorage.getTerm();
        raftStorage.update(term + 1, cluster.getLocalNodeId());
    }

    public Flux<Sender> availableSenders() {
        return cluster.availableSenders(this);
    }

    public String getGroupName() {
        return groupName;
    }

    public Duration getCurrentElectionTimeout() {
        return currentElectionTimeout;
    }

    public void appendEntriesCall() {
        this.lastAppendEntriesCall.set(System.currentTimeMillis());
    }

    public void addConfirmListener(ConfirmListener confirmListener) {
        confirmListeners.add(confirmListener);
    }

    public void removeConfirmListener(ConfirmListener confirmListener) {
        confirmListeners.remove(confirmListener);
    }

    public void addLastAppliedListener(LastAppliedListener lastAppliedListener) {
        lastAppliedListeners.add(lastAppliedListener);
    }

    public void removeLastAppliedListener(LastAppliedListener lastAppliedListener) {
        lastAppliedListeners.remove(lastAppliedListener);
    }

    void setCurrentLeader(int nodeId) {
        currentLeaderId.set(nodeId);
    }

    void resetCurrentLeader() {
        setCurrentLeader(0);
    }

    void convertToPassive() {
        transitionBetweenStates(this.nodeState.nodeState(), new PassiveRole());
    }

    void convertToFollower(int term) {
        if (term < raftStorage.getTerm()) {
            throw new RaftException(String.format("[Node %s] [current state %s] Term can only be increased! Current term %s vs %s.", cluster.getLocalNodeId(), nodeState, raftStorage.getTerm(), term));
        }
        if (term > raftStorage.getTerm()) {
            raftStorage.update(term, 0);
        }
        if (this.nodeState.nodeState() == NodeState.FOLLOWER) {
            return;
        }
        transitionBetweenStates(this.nodeState.nodeState(), new FollowerRole());
    }

    void refreshFollower() {
        transitionBetweenStates(this.nodeState.nodeState(), new FollowerRole());
    }

    void convertToCandidate() {
        resetCurrentLeader();
        transitionBetweenStates(NodeState.FOLLOWER, new CandidateRole());
    }

    void convertToLeader() {
        transitionBetweenStates(NodeState.CANDIDATE, new LeaderRole());
        setCurrentLeader(cluster.getLocalNodeId());
    }

    private void transitionBetweenStates(NodeState stateFrom, RaftRole raftRole) {
        if (nodeState.nodeState() != stateFrom) {
            throw new RaftException(String.format("[Node %s] [current state %s] Cannot transition from %s to %s.", cluster.getLocalNodeId(), nodeState, stateFrom, raftRole.nodeState()));
        }
        LOGGER.info("[Node {}][group {}] State transition {} -> {}", cluster.getLocalNodeId(), groupName, stateFrom, raftRole.nodeState());
        nodeState.onExit(cluster, this, raftStorage);
        nodeState = raftRole;
        nodeState.onInit(cluster, this, raftStorage);
    }

    public int getCurrentLeaderId() {
        return currentLeaderId.get();
    }

    public Mono<Payload> onClientRequest(Payload payload) {
        return nodeState.onClientRequest(cluster, this, this.raftStorage, payload);
    }

    public Flux<Payload> onClientRequests(Publisher<Payload> payloads) {
        return nodeState.onClientRequests(cluster, this, this.raftStorage, payloads);
    }

    public boolean lastAppendEntriesWithinElectionTimeout() {
        return System.currentTimeMillis() - lastAppendEntriesCall.get() < currentElectionTimeout.toMillis();
    }

    public boolean isLeader() {
        return nodeState.nodeState() == NodeState.LEADER;
    }

    public boolean isNotLeader() {
        return nodeState.nodeState() != NodeState.LEADER;
    }

    public boolean isFollower() {
        return nodeState.nodeState() == NodeState.FOLLOWER;
    }

    public boolean isCandidate() {
        return nodeState.nodeState() == NodeState.CANDIDATE;
    }

    public boolean isPassive() {
        return nodeState.nodeState() == NodeState.PASSIVE;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
        if (commitIndex >= currentConfigurationId && currentConfigurationId > previousConfigurationId) {
            LOGGER.info("[Node {}] Configuration {} committed", cluster.getLocalNodeId(), currentConfiguration);
            raftStorage.updateConfiguration(currentConfiguration);
            previousConfigurationId = currentConfigurationId;
        }
        confirmListeners.forEach(zomkyStorageConfirmListener -> zomkyStorageConfirmListener.handle(commitIndex));
        advanceStateMachineSink.next(commitIndex);
    }

    public Duration nextElectionTimeout() {
        this.currentElectionTimeout = raftConfiguration.getElectionTimeout().nextRandom();
        LOGGER.debug("[Node {}][Group {}] Current election timeout {}", cluster.getLocalNodeId(), groupName, currentElectionTimeout);
        return currentElectionTimeout;
    }

    public Mono<AppendEntriesResponse> onAppendEntries(AppendEntriesRequest appendEntries) {
        return nodeState.onAppendEntries(cluster, this, this.raftStorage, appendEntries);
    }

    public Mono<PreVoteResponse> onPreRequestVote(PreVoteRequest preRequestVote) {
        return nodeState.onPreRequestVote(cluster, this, this.raftStorage, preRequestVote);
    }

    public Mono<VoteResponse> onRequestVote(VoteRequest requestVote) {
        return nodeState.onRequestVote(cluster, this, this.raftStorage, requestVote);
    }

    public Mono<AddServerResponse> onAddServer(AddServerRequest addServerRequest) {
        return nodeState.onAddServer(cluster, this, this.raftStorage, addServerRequest);
    }

    public Mono<RemoveServerResponse> onRemoveServer(RemoveServerRequest removeServerRequest) {
        return nodeState.onRemoveServer(cluster, this, this.raftStorage, removeServerRequest);
    }

    public void markNewEntry(long index, long timestamp) {
        nodeState.markNewEntry(index, timestamp);
    }

    public void addConfigurationChangeListener(ConfigurationChangeListener configurationChangeListener) {
        configurationChangeListeners.add(configurationChangeListener);
    }

    void addServer(AddServerRequest addServerRequest) {
        waitUntilPreviousConfigurationFinished(() -> {
            // Append new configuration entry to log (old configuration plus new server),
            // commit it using majority of new configuration
            Configuration oldConfiguration = currentConfiguration;
            Configuration newConfiguration = oldConfiguration.addMember(addServerRequest.getNewServer());
            updateConfiguration(newConfiguration);
            //senders.addServer(addServerRequest.getNewServer()); // no longer valid with gossip
            configurationChangeListeners.forEach(configurationChangeListener -> configurationChangeListener.handle(oldConfiguration, newConfiguration));
        });
    }

    void removeServer(RemoveServerRequest removeServerRequest) {
        waitUntilPreviousConfigurationFinished(() -> {
            int oldMember = removeServerRequest.getOldServer();
            // Append new configuration entry to log (old configuration plus new server),
            // commit it using majority of new configuration.
            Configuration oldConfiguration = currentConfiguration;
            Configuration newConfiguration = oldConfiguration.removeMember(oldMember);
            updateConfiguration(newConfiguration);
            //senders.removeServer(oldMember); // no longer valid with gossip
            configurationChangeListeners.forEach(configurationChangeListener -> configurationChangeListener.handle(oldConfiguration, newConfiguration));

            // If this server was removed, step down.
            if (oldMember == cluster.getLocalNodeId()) {
                this.convertToPassive();
            }
        });
    }

    private void waitUntilPreviousConfigurationFinished(Runnable runnable) {
        configurationLock.lock();
        try {
            // Wait until previous configuration in log is committed.
            while (currentConfigurationId > previousConfigurationId) {

            }
            runnable.run();
        } finally {
            configurationLock.unlock();
        }
    }

    private void updateConfiguration(Configuration newConfiguration) {
        ConfigurationEntry configurationEntry = new ConfigurationEntry(raftStorage.getTerm(), System.currentTimeMillis(), newConfiguration.getMembers());
        IndexedLogEntry indexedLogEntry = raftStorage.append(configurationEntry);
        currentConfigurationId = indexedLogEntry.getIndex();
        currentConfiguration = newConfiguration;
    }

    public Configuration getCurrentConfiguration() {
        return currentConfiguration;
    }

    public int quorum() {
        return currentConfiguration.quorum();
    }

    // for followers
    public void apply(IndexedLogEntry indexedLogEntry) {
        LogEntry logEntry = indexedLogEntry.getLogEntry();
        if (logEntry instanceof ConfigurationEntry) {
            ConfigurationEntry configurationEntry = (ConfigurationEntry) logEntry;
            LOGGER.info("[Node {}] New configuration {}", cluster.getLocalNodeId(), configurationEntry.getMembers());
            currentConfiguration = new Configuration(configurationEntry.getMembers());
            currentConfigurationId = indexedLogEntry.getIndex();
//            senders.replaceWith(currentConfiguration);
            if (!currentConfiguration.getMembers().contains(cluster.getLocalNodeId())) {
                convertToPassive();
            }
        }
    }

    // for passive server that is being catched up
    public void applyPassive(IndexedLogEntry indexedLogEntry) {
        LogEntry logEntry = indexedLogEntry.getLogEntry();
        if (logEntry instanceof ConfigurationEntry) {
            ConfigurationEntry configurationEntry = (ConfigurationEntry) logEntry;
            LOGGER.info("[Node {}] New configuration {}", cluster.getLocalNodeId(), configurationEntry.getMembers());
            currentConfiguration = new Configuration(configurationEntry.getMembers());
            currentConfigurationId = indexedLogEntry.getIndex();
//            senders.replaceWith(currentConfiguration);
            if (currentConfiguration.getMembers().contains(cluster.getLocalNodeId())) {
                this.convertToFollower(raftStorage.getTerm());
            }
        }
    }

    private void advanceStateMachine(long commitIndex) {
        for (;;) {
            boolean hasNext = logStorageReader.hasNext();
            if (hasNext) {
                final IndexedLogEntry indexedLogEntry = logStorageReader.next();
                LOGGER.debug("[Server {}] advance state machine for group {}, next {}", cluster.getLocalNodeId(), groupName, indexedLogEntry);
                if (indexedLogEntry.getLogEntry() instanceof CommandEntry) {
                    ByteBuffer response = raftConfiguration.getStateMachine().applyLogEntry((CommandEntry) indexedLogEntry.getLogEntry());
                    lastAppliedListeners.forEach(lastAppliedListener -> lastAppliedListener.handle(indexedLogEntry.getIndex(), Unpooled.wrappedBuffer(response)));
                    LOGGER.debug("[Node {}, group {}] index {} has been applied to state machine", cluster.getLocalNodeId(), groupName, indexedLogEntry.getIndex());
                }
            } else {
                if (logStorageReader.getCurrentIndex() >= commitIndex) break;
            }
        }
    }

    public Mono<Sender> getSenderById(int newServer) {
        return cluster.getSenderById(newServer);
    }

    public static Builder builder() {
        return new Builder();
    }

    private RaftGroup() {}

    public static class Builder {
        private RaftStorage raftStorage;
        private RaftConfiguration raftConfiguration;
        private Cluster cluster;
        private String groupName;
        private RaftRole role;
        private MetricsCollector metricsCollector;

        private Builder() {
        }

        public Builder metricsCollector(MetricsCollector metricsCollector) {
            this.metricsCollector = metricsCollector;
            return this;
        }

        public Builder raftStorage(RaftStorage raftStorage) {
            this.raftStorage = raftStorage;
            return this;
        }

        public Builder inMemoryRaftStorage() {
            this.raftStorage = new InMemoryRaftStorage();
            return this;
        }

        public Builder raftConfiguration(RaftConfiguration raftConfiguration) {
            this.raftConfiguration = raftConfiguration;
            return this;
        }

        public Builder cluster(Cluster cluster) {
            this.cluster = cluster;
            return this;
        }

        public Builder groupName(String groupName) {
            this.groupName = groupName;
            return this;
        }

        public Builder raftRole(RaftRole raftRole) {
            this.role = raftRole;
            return this;
        }

        public RaftGroup build() {
            RaftGroup raftGroup = new RaftGroup();
            raftGroup.metrics = metricsCollector;
            raftGroup.raftStorage = raftStorage;
            raftGroup.logStorageReader = raftStorage.openReader(() -> raftGroup.commitIndex);
            raftGroup.cluster = cluster;
            raftGroup.raftConfiguration = raftConfiguration;
            raftGroup.nodeState = role;
            raftGroup.groupName = groupName;
            raftGroup.currentConfiguration = raftStorage.getConfiguration();
            if (raftGroup.currentConfiguration == null) {
                raftGroup.currentConfiguration = raftConfiguration.getConfiguration();
                this.raftStorage.updateConfiguration(raftConfiguration.getConfiguration());
            }
            return raftGroup;
        }

    }
}