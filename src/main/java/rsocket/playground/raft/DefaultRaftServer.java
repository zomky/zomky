package rsocket.playground.raft;

import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rsocket.playground.raft.listener.LastAppliedListener;
import rsocket.playground.raft.rpc.AppendEntriesRequest;
import rsocket.playground.raft.rpc.AppendEntriesResponse;
import rsocket.playground.raft.rpc.VoteRequest;
import rsocket.playground.raft.rpc.VoteResponse;
import rsocket.playground.raft.listener.ConfirmListener;
import rsocket.playground.raft.storage.RaftStorage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class DefaultRaftServer implements RaftServer {

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

    StateMachine stateMachine;
    private ScheduledExecutorService stateMachineExecutor;

    volatile RaftServerRole nodeState = new FollowerRole();

    int nodeId;

    private AtomicInteger currentLeaderId = new AtomicInteger(0);

    /**
     * index of highest log entry known to be
     * committed (initialized to 0, increases
     * monotonically)
     */
    private AtomicLong commitIndex = new AtomicLong(0);

    /**
     * index of highest log entry applied to state
     * machine (initialized to 0, increases
     * monotonically)
     */
    private AtomicLong lastApplied = new AtomicLong(0);

    private Receiver receiver;
    private Senders senders;
    private RaftStorage raftStorage;

    private Set<SenderAvailableCallback> senderAvailableCallbacks = new HashSet<>();
    private Set<SenderUnavailableCallback> senderUnavailableCallbacks = new HashSet<>();

    private List<ConfirmListener> confirmListeners = new ArrayList<>();
    private List<LastAppliedListener> lastAppliedListeners = new ArrayList<>();

    ElectionTimeout electionTimeout;

    DefaultRaftServer(int port, RaftStorage raftStorage, List<Integer> clientPorts, StateMachine stateMachine, ElectionTimeout electionTimeout) {
        this.nodeId = port;
        this.raftStorage = raftStorage;
        this.stateMachine = stateMachine;
        this.electionTimeout = electionTimeout;
        this.receiver = new Receiver(this);
        this.senders = new Senders(this, clientPorts);
        LOGGER.info("[RaftServer {}] has been initialized", nodeId);
    }

    public void start() {
        receiver.start();
        senders.start();
        if (stateMachine != null) {
            stateMachineExecutor = Executors.newSingleThreadScheduledExecutor();
            stateMachineExecutor.scheduleWithFixedDelay(() -> {
                while (lastApplied.get() < commitIndex.get()) {
                    LOGGER.info("[RaftServer {}] index {} has been applied to state machine", nodeId, lastApplied.get() + 1);
                    ByteBuffer response = stateMachine.applyLogEntry(raftStorage.getEntryByIndex(lastApplied.incrementAndGet()));
                    lastAppliedListeners.forEach(lastAppliedListener -> lastAppliedListener.handle(lastApplied.get(), Unpooled.wrappedBuffer(response)));
                }
            }, 0, 10, TimeUnit.MILLISECONDS);
        }
        nodeState.onInit(this, raftStorage);
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

    boolean notVoted(long term) {
        return term > raftStorage.getTerm() || (term == raftStorage.getTerm() && raftStorage.getVotedFor() == 0);
    }

    public long getCommitIndex() {
        return commitIndex.get();
    }

    public void setCommitIndex(long commitIndex) {
        LOGGER.info("[RaftServer {}] Set new commit index to {}", nodeId, commitIndex);
        this.commitIndex.set(commitIndex);
        confirmListeners.forEach(zomkyStorageConfirmListener -> zomkyStorageConfirmListener.handle(commitIndex));
    }

    public void addConfirmListener(ConfirmListener zomkyStorageConfirmListener) {
        confirmListeners.add(zomkyStorageConfirmListener);
    }

    public void addLastAppliedListener(LastAppliedListener lastAppliedListener) {
        lastAppliedListeners.add(lastAppliedListener);
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

    Mono<AppendEntriesResponse> onAppendEntries(AppendEntriesRequest appendEntries) {
        return nodeState.onAppendEntries(this, raftStorage, appendEntries)
                .doOnNext(response -> setCurrentLeader(appendEntries.getLeaderId()))
                .doOnNext(response -> {
                    if (appendEntries.getEntriesSize() > 0) {
                        LOGGER.info("[RaftServer {} -> RaftServer {}] Append entries \n{} \n-> \n{}", appendEntries.getLeaderId(), nodeId, appendEntries, response);
                    }
                });
    }

    Mono<VoteResponse> onRequestVote(VoteRequest requestVote) {
        return nodeState.onRequestVote(this, raftStorage, requestVote)
                .doOnNext(voteResponse -> LOGGER.info("[RaftServer {} -> RaftServer {}] Vote \n{} \n-> \n{}", requestVote.getCandidateId(), nodeId, requestVote, voteResponse));
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

}
