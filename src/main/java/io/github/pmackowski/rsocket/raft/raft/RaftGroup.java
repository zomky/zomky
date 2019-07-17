package io.github.pmackowski.rsocket.raft.raft;

import io.github.pmackowski.rsocket.raft.InnerNode;
import io.github.pmackowski.rsocket.raft.Node;
import io.github.pmackowski.rsocket.raft.listener.ConfigurationChangeListener;
import io.github.pmackowski.rsocket.raft.listener.ConfirmListener;
import io.github.pmackowski.rsocket.raft.listener.LastAppliedListener;
import io.github.pmackowski.rsocket.raft.storage.InMemoryRaftStorage;
import io.github.pmackowski.rsocket.raft.storage.RaftStorage;
import io.github.pmackowski.rsocket.raft.storage.log.entry.CommandEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.ConfigurationEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedLogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.LogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.reader.LogStorageReader;
import io.github.pmackowski.rsocket.raft.storage.meta.Configuration;
import io.github.pmackowski.rsocket.raft.transport.Sender;
import io.github.pmackowski.rsocket.raft.transport.protobuf.*;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public class RaftGroup {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftGroup.class);

    RaftStorage raftStorage;
    private InnerNode node;
    private String groupName;
    private RaftConfiguration raftConfiguration;
    private LogStorageReader logStorageReader;

    private RaftRole nodeState = new FollowerRole();
    private AtomicInteger currentLeaderId = new AtomicInteger(0);
    private AtomicLong lastApplied = new AtomicLong(0);
    private AtomicLong lastAppendEntriesCall = new AtomicLong(0); // maybe globally for all groups ?
    private volatile Duration currentElectionTimeout = Duration.ofMillis(0);

    private List<ConfirmListener> confirmListeners = new ArrayList<>();
    private List<LastAppliedListener> lastAppliedListeners = new ArrayList<>();

    private List<ConfigurationChangeListener> configurationChangeListeners = new ArrayList<>();
    private volatile Configuration currentConfiguration;
    private volatile long previousConfigurationId;
    private volatile long currentConfigurationId;

    Lock configurationLock = new ReentrantLock();

    public RaftConfiguration getRaftConfiguration() {
        return raftConfiguration;
    }

    void onInit() {
        nodeState.onInit(node, this, raftStorage);
    }

    public boolean isPreVote() {
        return raftConfiguration.isPreVote();
    }

    public boolean isLeaderStickiness() {
        return raftConfiguration.isLeaderStickiness();
    }

    void voteForMyself() {
        int term = raftStorage.getTerm();
        raftStorage.update(term + 1, node.getNodeId());
    }

    public Flux<Sender> availableSenders() {
        return node.getSenders().availableSenders(this);
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
            throw new RaftException(String.format("[Node %s] [current state %s] Term can only be increased! Current term %s vs %s.", node.getNodeId(), nodeState, raftStorage.getTerm(), term));
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
        setCurrentLeader(node.getNodeId());
    }

    private void transitionBetweenStates(NodeState stateFrom, RaftRole raftRole) {
        if (nodeState.nodeState() != stateFrom) {
            throw new RaftException(String.format("[Node %s] [current state %s] Cannot transition from %s to %s.", node.getNodeId(), nodeState, stateFrom, raftRole.nodeState()));
        }
        LOGGER.info("[Node {}] State transition {} -> {}", node.getNodeId(), stateFrom, raftRole.nodeState());
        nodeState.onExit(node, this, raftStorage);
        nodeState = raftRole;
        nodeState.onInit(node, this, raftStorage);
    }

    public int getCurrentLeaderId() {
        return currentLeaderId.get();
    }

    public void onExit() {

    }

    public Mono<Payload> onClientRequest(Payload payload) {
        return nodeState.onClientRequest(node, this, this.raftStorage, payload);
    }

    public Flux<Payload> onClientRequests(Publisher<Payload> payloads) {
        return nodeState.onClientRequests(node, this, this.raftStorage, payloads);
    }

    public boolean lastAppendEntriesWithinElectionTimeout() {
        return System.currentTimeMillis() - lastAppendEntriesCall.get() < currentElectionTimeout.toMillis();
    }

    public boolean isLeader() {
        return nodeState.nodeState() == NodeState.LEADER;
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
        return raftStorage.commitIndex();
    }

    public void setCommitIndex(long commitIndex) {
        raftStorage.commit(commitIndex);
        if (commitIndex >= currentConfigurationId && currentConfigurationId > previousConfigurationId) {
            LOGGER.info("[Node {}] Configuration {} committed", node.getNodeId(), currentConfiguration);
            raftStorage.updateConfiguration(currentConfiguration);
            previousConfigurationId = currentConfigurationId;
        }
        confirmListeners.forEach(zomkyStorageConfirmListener -> zomkyStorageConfirmListener.handle(commitIndex));
    }

    public Duration nextElectionTimeout() {
        this.currentElectionTimeout = raftConfiguration.getElectionTimeout().nextRandom();
        LOGGER.info("[Node {}] Current election timeout {}", node.getNodeId(), currentElectionTimeout);
        return currentElectionTimeout;
    }

    public Mono<AppendEntriesResponse> onAppendEntries(AppendEntriesRequest appendEntries) {
        return nodeState.onAppendEntries(node, this, this.raftStorage, appendEntries);
    }

    public Mono<PreVoteResponse> onPreRequestVote(PreVoteRequest preRequestVote) {
        return nodeState.onPreRequestVote(node, this, this.raftStorage, preRequestVote);
    }

    public Mono<VoteResponse> onRequestVote(VoteRequest requestVote) {
        return nodeState.onRequestVote(node, this, this.raftStorage, requestVote);
    }

    public Mono<AddServerResponse> onAddServer(AddServerRequest addServerRequest) {
        return nodeState.onAddServer(node, this, this.raftStorage, addServerRequest);
    }

    public Mono<RemoveServerResponse> onRemoveServer(RemoveServerRequest removeServerRequest) {
        return nodeState.onRemoveServer(node, this, this.raftStorage, removeServerRequest);
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
            if (oldMember == node.getNodeId()) {
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
            LOGGER.info("[Node {}] New configuration {}", node.getNodeId(), configurationEntry.getMembers());
            currentConfiguration = new Configuration(configurationEntry.getMembers());
            currentConfigurationId = indexedLogEntry.getIndex();
//            senders.replaceWith(currentConfiguration);
            if (!currentConfiguration.getMembers().contains(node.getNodeId())) {
                convertToPassive();
            }
        }
    }

    // for passive server that is being catched up
    public void applyPassive(IndexedLogEntry indexedLogEntry) {
        LogEntry logEntry = indexedLogEntry.getLogEntry();
        if (logEntry instanceof ConfigurationEntry) {
            ConfigurationEntry configurationEntry = (ConfigurationEntry) logEntry;
            LOGGER.info("[Node {}] New configuration {}", node.getNodeId(), configurationEntry.getMembers());
            currentConfiguration = new Configuration(configurationEntry.getMembers());
            currentConfigurationId = indexedLogEntry.getIndex();
//            senders.replaceWith(currentConfiguration);
            if (currentConfiguration.getMembers().contains(node.getNodeId())) {
                this.convertToFollower(raftStorage.getTerm());
            }
        }
    }

    public void advanceStateMachine() {

        // very inefficient
        while (logStorageReader.hasNext()) {
            final IndexedLogEntry indexedLogEntry = logStorageReader.next();
            LOGGER.info("[Server {}] advance state machine for group {}, next {}", node.getNodeId(), groupName, indexedLogEntry);
            if (indexedLogEntry.getLogEntry() instanceof CommandEntry) {
                ByteBuffer response = raftConfiguration.getStateMachine().applyLogEntry(indexedLogEntry.getLogEntry());
                lastAppliedListeners.forEach(lastAppliedListener -> lastAppliedListener.handle(indexedLogEntry.getIndex(), Unpooled.wrappedBuffer(response)));
                LOGGER.info("[Node {}, group {}] index {} has been applied to state machine", node.getNodeId(), groupName, indexedLogEntry.getIndex());
            }
        }
        logStorageReader.close();
    }

    public Mono<Sender> getSenderById(int newServer) {
        return node.getSenders().getSenderById(newServer);
    }

    public static Builder builder() {
        return new Builder();
    }


    private RaftGroup() {}

    public static class Builder {
        private RaftStorage raftStorage;
        private RaftConfiguration raftConfiguration;
        private InnerNode node;
        private String groupName;
        private Configuration configuration;

        private Builder() {
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


        public Builder raftConfiguration(RaftConfiguration.Builder defaultBuilder, Function<RaftConfiguration.Builder,RaftConfiguration.Builder> builderFunction) {
            this.raftConfiguration = builderFunction.apply(defaultBuilder).build();
            return this;
        }

        public Builder node(Node node) {
            this.node = (InnerNode) node;
            return this;
        }

        public Builder groupName(String groupName) {
            this.groupName = groupName;
            return this;
        }

        public Builder configuration(Configuration configuration) {
            this.configuration = configuration;
            return this;
        }

        public RaftGroup build() {
            RaftGroup raftGroup = new RaftGroup();
            raftGroup.raftStorage = raftStorage;
            raftGroup.logStorageReader = raftStorage.openCommittedEntriesReader();
            raftGroup.raftConfiguration = raftConfiguration;
            raftGroup.node = node;
            raftGroup.groupName = groupName;
            raftGroup.currentConfiguration = raftStorage.getConfiguration();
            if (raftGroup.currentConfiguration == null) {
                raftGroup.currentConfiguration = configuration;
                this.raftStorage.updateConfiguration(configuration);
            }
            return raftGroup;
        }

    }
}