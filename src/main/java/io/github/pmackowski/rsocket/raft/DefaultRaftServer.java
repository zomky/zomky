package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.listener.ConfigurationChangeListener;
import io.github.pmackowski.rsocket.raft.listener.ConfirmListener;
import io.github.pmackowski.rsocket.raft.listener.LastAppliedListener;
import io.github.pmackowski.rsocket.raft.rpc.*;
import io.github.pmackowski.rsocket.raft.storage.RaftStorage;
import io.github.pmackowski.rsocket.raft.storage.log.entry.CommandEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.ConfigurationEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedLogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.LogEntry;
import io.github.pmackowski.rsocket.raft.storage.log.reader.LogStorageReader;
import io.github.pmackowski.rsocket.raft.storage.meta.Configuration;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class DefaultRaftServer implements InternalRaftServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultRaftServer.class);

    DefaultRaftServer() {}

    @Override
    public Mono<Void> onClose() {
        return null;
    }

    @Override
    public void dispose() {
        LOGGER.info("[RaftServer {}] Stopping {} ...", nodeId, nodeState.nodeState());
        nodeState.onExit(this, raftStorage);

        receiver.stop();
        senders.stop();
        stateMachineExecutor.shutdownNow();
    }

    @Override
    public boolean isLeader() {
        return nodeState.nodeState() == NodeState.LEADER;
    }

    @Override
    public boolean isFollower() {
        return nodeState.nodeState() == NodeState.FOLLOWER;
    }

    @Override
    public boolean isCandidate() {
        return nodeState.nodeState() == NodeState.CANDIDATE;
    }

    StateMachine<ByteBuffer> stateMachine;
    private ScheduledExecutorService stateMachineExecutor;

    volatile RaftServerRole nodeState = new FollowerRole();

    int nodeId;

    private AtomicInteger currentLeaderId = new AtomicInteger(0);

    /**
     * index of highest log entry applied to state
     * machine (initialized to 0, increases
     * monotonically)
     */
    private AtomicLong lastApplied = new AtomicLong(0);

    private Receiver receiver;
    private Senders senders;
    RaftStorage raftStorage;
    private boolean preVote;
    private boolean leaderStickiness;
    private AtomicLong lastAppendEntriesCall = new AtomicLong(0);
    private volatile Duration currentElectionTimeout = Duration.ofMillis(0);

    private Set<SenderAvailableCallback> senderAvailableCallbacks = new HashSet<>();
    private Set<SenderUnavailableCallback> senderUnavailableCallbacks = new HashSet<>();

    private List<ConfirmListener> confirmListeners = new ArrayList<>();
    private List<LastAppliedListener> lastAppliedListeners = new ArrayList<>();
    private List<ConfigurationChangeListener> configurationChangeListeners = new ArrayList<>();

    private volatile Configuration currentConfiguration;
    private volatile long previousConfigurationId;
    private volatile long currentConfigurationId;

    ElectionTimeout electionTimeout;

    DefaultRaftServer(int port,
                      RaftStorage raftStorage,
                      Configuration initialConfiguration,
                      StateMachine<ByteBuffer> stateMachine,
                      ElectionTimeout electionTimeout,
                      boolean preVote,
                      boolean leaderStickiness) {
        this.nodeId = port;
        this.raftStorage = raftStorage;
        this.stateMachine = stateMachine;
        this.electionTimeout = electionTimeout;
        this.preVote = preVote;
        this.leaderStickiness = leaderStickiness;
        this.currentConfiguration = raftStorage.getConfiguration();
        if (currentConfiguration == null) {
            this.raftStorage.updateConfiguration(initialConfiguration);
            this.currentConfiguration = initialConfiguration;
        }

        this.receiver = new Receiver(this);
        this.senders = new Senders(this);
        LOGGER.info("[RaftServer {}] has been initialized", nodeId);
    }

    public void start() {
        receiver.start();
        senders.start();
        nodeState.onInit(this, raftStorage);

        final LogStorageReader logStorageReader = raftStorage.openCommittedEntriesReader();
        stateMachineExecutor = Executors.newScheduledThreadPool(1);
        stateMachineExecutor.scheduleWithFixedDelay(() -> {
            try {
                while (logStorageReader.hasNext()) {
                    final IndexedLogEntry indexedLogEntry = logStorageReader.next();
                    LOGGER.info("next {}", indexedLogEntry);
                    if (indexedLogEntry.getLogEntry() instanceof CommandEntry) {
                        ByteBuffer response = stateMachine.applyLogEntry(indexedLogEntry.getLogEntry());
                        lastAppliedListeners.forEach(lastAppliedListener -> lastAppliedListener.handle(indexedLogEntry.getIndex(), Unpooled.wrappedBuffer(response)));
                        LOGGER.info("[RaftServer {}] index {} has been applied to state machine", nodeId, indexedLogEntry.getIndex());
                    }
                }
            } catch (Exception e) {
                LOGGER.error("Main loop failure", e);
            }
        }, 0, 10, TimeUnit.MILLISECONDS);
    }

    Flux<Sender> availableSenders() {
        return senders.availableSenders();
    }

    Mono<Payload> onClientRequest(Payload payload) {
        return nodeState.onClientRequest(this, raftStorage, payload);
    }

    Flux<Payload> onClientRequests(Publisher<Payload> payloads) {
        return nodeState.onClientRequests(this, raftStorage, payloads);
    }

    void voteForMyself() {
        int term = raftStorage.getTerm();
        raftStorage.update(term + 1, nodeId);
    }

    @Override
    public ElectionTimeout getElectionTimeout() {
        return electionTimeout;
    }

    public long getCommitIndex() {
        return raftStorage.commitIndex();
    }

    public void setCommitIndex(long commitIndex) {
        raftStorage.commit(commitIndex);
        if (commitIndex >= currentConfigurationId && currentConfigurationId > previousConfigurationId) {
            LOGGER.info("[RaftServer {}] Configuration {} committed", nodeId, currentConfiguration);
            raftStorage.updateConfiguration(currentConfiguration);
            previousConfigurationId = currentConfigurationId;
        }
        confirmListeners.forEach(zomkyStorageConfirmListener -> zomkyStorageConfirmListener.handle(commitIndex));
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

    public void addConfigurationChangeListener(ConfigurationChangeListener configurationChangeListener) {
        configurationChangeListeners.add(configurationChangeListener);
    }

    void onSenderAvailable(SenderAvailableCallback senderAvailableCallback) {
        senderAvailableCallbacks.add(senderAvailableCallback);
    }

    void onSenderUnavailable(SenderUnavailableCallback senderUnavailableCallback) {
        senderUnavailableCallbacks.add(senderUnavailableCallback);
    }

    void senderAvailable(Sender sender) {
        senderAvailableCallbacks.forEach(senderAvailableCallback -> senderAvailableCallback.handle(sender));
    }

    void senderUnavailable(Sender sender) {
        senderUnavailableCallbacks.forEach(senderUnavailableCallback -> senderUnavailableCallback.handle(sender));
    }

    void setCurrentLeader(int nodeId) {
        currentLeaderId.set(nodeId);
    }

    void resetCurrentLeader() {
        setCurrentLeader(0);
    }

    @Override
    public Mono<AppendEntriesResponse> onAppendEntries(AppendEntriesRequest appendEntries) {
        return nodeState.onAppendEntries(this, raftStorage, appendEntries)
                .doOnNext(response -> setCurrentLeader(appendEntries.getLeaderId()))
                .doOnNext(response -> {
                    if (appendEntries.getEntriesCount() > 0) {
                        LOGGER.info("[RaftServer {} -> RaftServer {}] Append entries \n{} \n-> \n{}", appendEntries.getLeaderId(), nodeId, appendEntries, response);
                    }
                });
    }

    Mono<PreVoteResponse> onPreRequestVote(PreVoteRequest preRequestVote) {
        return nodeState.onPreRequestVote(this, raftStorage, preRequestVote)
                .doOnNext(preVoteResponse -> LOGGER.info("[RaftServer {} -> RaftServer {}] Pre-Vote \n{} \n-> \n{}", preRequestVote.getCandidateId(), nodeId, preRequestVote, preVoteResponse));
    }

    Mono<VoteResponse> onRequestVote(VoteRequest requestVote) {
        return nodeState.onRequestVote(this, raftStorage, requestVote)
                .doOnNext(voteResponse -> LOGGER.info("[RaftServer {} -> RaftServer {}] Vote \n{} \n-> \n{}", requestVote.getCandidateId(), nodeId, requestVote, voteResponse));
    }

    void convertToPassive() {
        transitionBetweenStates(this.nodeState.nodeState(), new PassiveRole());
    }

    void convertToFollower(int term) {
        if (term < raftStorage.getTerm()) {
            throw new RaftException(String.format("[RaftServer %s] [current state %s] Term can only be increased! Current term %s vs %s.", nodeId, nodeState, raftStorage.getTerm(), term));
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
            throw new RaftException(String.format("[RaftServer %s] [current state %s] Cannot transition from %s to %s.", nodeId, nodeState, stateFrom, raftServerRole.nodeState()));
        }
        LOGGER.info("[RaftServer {}] State transition {} -> {}", nodeId, stateFrom, raftServerRole.nodeState());
        nodeState.onExit(this, raftStorage);
        nodeState = raftServerRole;
        nodeState.onInit(this, raftStorage);
    }

    public int getCurrentLeaderId() {
        return currentLeaderId.get();
    }

    public boolean preVote() {
        return preVote;
    }

    public boolean leaderStickiness() {
        return leaderStickiness;
    }

    public Duration nextElectionTimeout() {
        this.currentElectionTimeout = electionTimeout.nextRandom();
        LOGGER.info("[RaftServer {}] Current election timeout {}", nodeId, currentElectionTimeout);
        return currentElectionTimeout;
    }

    public boolean lastAppendEntriesWithinElectionTimeout() {
        return System.currentTimeMillis() - lastAppendEntriesCall.get() < currentElectionTimeout.toMillis();
    }

    public Mono<Payload> onAddServer(Payload payload) {
        return nodeState.onAddServer(this, raftStorage, payload);
    }

    @Override
    public void addServer(int newMember) {
        // 1. Reply NOT_LEADER if not leader
        if (!isLeader()) {
            throw new RaftException("Leader can only add server!");
        }

        // 2. Catch up new server for fixed number of rounds. Reply TIMEOUT
        // if new server does not make progress for an election timeout or
        // if the last round takes longer than election timeout

        // 3. Wait until previous configuration in log is committed.
        if (currentConfigurationId > previousConfigurationId) {
            throw new RaftException("Other configure request is in progress!");
        }

        // 4. Append new configuration entry to log (old configuration plus new server),
        // commit it using majority of new configuration

        Configuration oldConfiguration = currentConfiguration;
        Configuration newConfiguration = oldConfiguration.addMember(newMember);
        ConfigurationEntry configurationEntry = new ConfigurationEntry(raftStorage.getTerm(), System.currentTimeMillis(), newConfiguration.getMembers());
        IndexedLogEntry indexedLogEntry = raftStorage.append(configurationEntry);
        currentConfigurationId = indexedLogEntry.getIndex();
        currentConfiguration = newConfiguration;
        senders.addServer(newMember);
        configurationChangeListeners.forEach(configurationChangeListener -> configurationChangeListener.handle(oldConfiguration, newConfiguration));
    }

    @Override
    public void removeServer(int oldMember) {
        // 1. Reply NOT_LEADER if not leader
        if (!isLeader()) {
            throw new RaftException("Leader can only remove server!");
        }

        // 2. Wait until previous configuration in log is committed.
        if (currentConfigurationId > previousConfigurationId) {
            throw new RaftException("Other configure request is in progress!");
        }

        // 3. Append new configuration entry to log (old configuration plus new server),
        // commit it using majority of new configuration.

        Configuration oldConfiguration = currentConfiguration;
        Configuration newConfiguration = oldConfiguration.removeMember(oldMember);
        ConfigurationEntry configurationEntry = new ConfigurationEntry(raftStorage.getTerm(), System.currentTimeMillis(), newConfiguration.getMembers());
        IndexedLogEntry indexedLogEntry = raftStorage.append(configurationEntry);
        currentConfigurationId = indexedLogEntry.getIndex();
        currentConfiguration = newConfiguration;
        senders.removeServer(oldMember);
        configurationChangeListeners.forEach(configurationChangeListener -> configurationChangeListener.handle(oldConfiguration, newConfiguration));

        // 4. If this server was removed, step down.
        if (oldMember == nodeId) {
            convertToPassive();
        }
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
            LOGGER.info("[RaftServer {}] New configuration {}", nodeId, configurationEntry.getMembers());
            currentConfiguration = new Configuration(configurationEntry.getMembers());
            currentConfigurationId = indexedLogEntry.getIndex();
            senders.replaceWith(currentConfiguration);
        }
    }
}
