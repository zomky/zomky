package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.listener.ConfigurationChangeListener;
import io.github.pmackowski.rsocket.raft.listener.ConfirmListener;
import io.github.pmackowski.rsocket.raft.listener.LastAppliedListener;
import io.github.pmackowski.rsocket.raft.storage.RaftStorage;
import io.github.pmackowski.rsocket.raft.storage.log.entry.CommandEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.ConfigurationEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedLogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.LogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.reader.LogStorageReader;
import io.github.pmackowski.rsocket.raft.storage.meta.Configuration;
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

public class RaftGroup {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftGroup.class);

    RaftStorage raftStorage;
    DefaultNode defaultRaftServer;
    int nodeId;
    String groupName;
    LogStorageReader logStorageReader;

    public RaftGroup() {
    }

    public RaftGroup(RaftStorage raftStorage, String groupName, List<Integer> nodes) {

    }

    public RaftGroup(RaftStorage raftStorage, DefaultNode defaultRaftServer, String groupName, List<Integer> nodes) {
        this.raftStorage = raftStorage;
        this.logStorageReader = raftStorage.openCommittedEntriesReader();
        this.defaultRaftServer = defaultRaftServer;
        this.nodeId = defaultRaftServer.nodeId;
        this.groupName = groupName;
        this.currentConfiguration = raftStorage.getConfiguration();
        if (currentConfiguration == null) {
            this.currentConfiguration = new Configuration(nodes);
            this.raftStorage.updateConfiguration(currentConfiguration);
        }
//        if (defaultRaftServer.isPassive()) {
//            this.nodeState = new PassiveRole();
//        }
    }

    private RaftServerRole nodeState = new FollowerRole();
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

//    void onInit(DefaultNode raftServer, RaftStorage raftStorage) {
    void onInit(DefaultNode raftServer) {
        nodeState.onInit(raftServer, this, raftStorage);
    }

    void voteForMyself() {
        int term = raftStorage.getTerm();
        raftStorage.update(term + 1, nodeId);
    }

    public String getGroupName() {
        return groupName;
    }

    public void appendEntriesCall() {
        this.lastAppendEntriesCall.set(System.currentTimeMillis());
    }

    public void addConfirmListener(ConfirmListener zomkyStorageConfirmListener) {
        confirmListeners.add(zomkyStorageConfirmListener);
    }

    public void addLastAppliedListener(LastAppliedListener lastAppliedListener) {
        lastAppliedListeners.add(lastAppliedListener);
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
            throw new RaftException(String.format("[Node %s] [current state %s] Term can only be increased! Current term %s vs %s.", nodeId, nodeState, raftStorage.getTerm(), term));
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
        setCurrentLeader(nodeId);
    }

    private void transitionBetweenStates(NodeState stateFrom, RaftServerRole raftServerRole) {
        if (nodeState.nodeState() != stateFrom) {
            throw new RaftException(String.format("[Node %s] [current state %s] Cannot transition from %s to %s.", nodeId, nodeState, stateFrom, raftServerRole.nodeState()));
        }
        LOGGER.info("[Node {}] State transition {} -> {}", nodeId, stateFrom, raftServerRole.nodeState());
        nodeState.onExit(defaultRaftServer, this, raftStorage);
        nodeState = raftServerRole;
        nodeState.onInit(defaultRaftServer, this, raftStorage);
    }

    public int getCurrentLeaderId() {
        return currentLeaderId.get();
    }

    public void onExit(DefaultNode raftServer, RaftStorage raftStorage) {

    }

    public Mono<Payload> onClientRequest(InnerNode node, Payload payload) {
        return nodeState.onClientRequest(defaultRaftServer, this, this.raftStorage, payload);
    }

    public Flux<Payload> onClientRequests(DefaultNode defaultRaftServer, RaftStorage raftStorage, Publisher<Payload> payloads) {
        return nodeState.onClientRequests(defaultRaftServer, this, this.raftStorage, payloads);
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
            LOGGER.info("[Node {}] Configuration {} committed", nodeId, currentConfiguration);
            raftStorage.updateConfiguration(currentConfiguration);
            previousConfigurationId = currentConfigurationId;
        }
        confirmListeners.forEach(zomkyStorageConfirmListener -> zomkyStorageConfirmListener.handle(commitIndex));
    }

    public Duration nextElectionTimeout() {
        this.currentElectionTimeout = defaultRaftServer.getElectionTimeout().nextRandom();
        LOGGER.info("[Node {}] Current election timeout {}", nodeId, currentElectionTimeout);
        return currentElectionTimeout;
    }

    public Mono<AppendEntriesResponse> onAppendEntries(DefaultNode defaultRaftServer, RaftStorage raftStorage, AppendEntriesRequest appendEntries) {
        return nodeState.onAppendEntries(defaultRaftServer, this, this.raftStorage, appendEntries);
    }

    public Mono<PreVoteResponse> onPreRequestVote(DefaultNode defaultRaftServer, RaftStorage raftStorage, PreVoteRequest preRequestVote) {
        return nodeState.onPreRequestVote(defaultRaftServer, this, this.raftStorage, preRequestVote);
    }

    public Mono<VoteResponse> onRequestVote(DefaultNode defaultRaftServer, RaftStorage raftStorage, VoteRequest requestVote) {
        return nodeState.onRequestVote(defaultRaftServer, this, this.raftStorage, requestVote);
    }

    public Mono<AddServerResponse> onAddServer(DefaultNode defaultRaftServer, RaftStorage raftStorage, AddServerRequest addServerRequest) {
        return nodeState.onAddServer(defaultRaftServer, this, this.raftStorage, addServerRequest);
    }

    public Mono<RemoveServerResponse> onRemoveServer(DefaultNode defaultRaftServer, RaftStorage raftStorage, RemoveServerRequest removeServerRequest) {
        return nodeState.onRemoveServer(defaultRaftServer, this, this.raftStorage, removeServerRequest);
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
            if (oldMember == nodeId) {
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
            LOGGER.info("[Node {}] New configuration {}", nodeId, configurationEntry.getMembers());
            currentConfiguration = new Configuration(configurationEntry.getMembers());
            currentConfigurationId = indexedLogEntry.getIndex();
//            senders.replaceWith(currentConfiguration);
            if (!currentConfiguration.getMembers().contains(nodeId)) {
                convertToPassive();
            }
        }
    }

    // for passive server that is being catched up
    public void applyPassive(IndexedLogEntry indexedLogEntry) {
        LogEntry logEntry = indexedLogEntry.getLogEntry();
        if (logEntry instanceof ConfigurationEntry) {
            ConfigurationEntry configurationEntry = (ConfigurationEntry) logEntry;
            LOGGER.info("[Node {}] New configuration {}", nodeId, configurationEntry.getMembers());
            currentConfiguration = new Configuration(configurationEntry.getMembers());
            currentConfigurationId = indexedLogEntry.getIndex();
//            senders.replaceWith(currentConfiguration);
            if (currentConfiguration.getMembers().contains(nodeId)) {
                this.convertToFollower(raftStorage.getTerm());
            }
        }
    }

    public void advanceStateMachine(DefaultNode defaultRaftServer) {

        // very inefficient
        while (logStorageReader.hasNext()) {
            final IndexedLogEntry indexedLogEntry = logStorageReader.next();
            LOGGER.info("[Server {}] advance state machine for group {}, next {}", defaultRaftServer.nodeId, groupName, indexedLogEntry);
            if (indexedLogEntry.getLogEntry() instanceof CommandEntry) {
                ByteBuffer response = defaultRaftServer.getStateMachine().applyLogEntry(indexedLogEntry.getLogEntry());
                lastAppliedListeners.forEach(lastAppliedListener -> lastAppliedListener.handle(indexedLogEntry.getIndex(), Unpooled.wrappedBuffer(response)));
                LOGGER.info("[Node {}, group {}] index {} has been applied to state machine", nodeId, groupName, indexedLogEntry.getIndex());
            }
        }
        logStorageReader.close();
    }
}
