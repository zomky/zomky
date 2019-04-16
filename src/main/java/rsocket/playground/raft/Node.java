package rsocket.playground.raft;

import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rsocket.playground.raft.rpc.AppendEntriesRequest;
import rsocket.playground.raft.rpc.AppendEntriesResponse;
import rsocket.playground.raft.rpc.VoteRequest;
import rsocket.playground.raft.rpc.VoteResponse;
import rsocket.playground.raft.storage.ZomkyConfirmListener;
import rsocket.playground.raft.storage.ZomkyStorage;

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

public class Node {

    private static final Logger LOGGER = LoggerFactory.getLogger(Node.class);

    StateMachine stateMachine;
    private ScheduledExecutorService stateMachineExecutor = Executors.newSingleThreadScheduledExecutor();

    volatile NodeState nodeState = NodeState.FOLLOWER;
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
    private ZomkyStorage zomkyStorage;

    private Set<SenderAvailableCallback> senderAvailableCallbacks = new HashSet<>();
    private Set<SenderUnavailableCallback> senderUnavailableCallbacks = new HashSet<>();

    private List<ZomkyConfirmListener> zomkyConfirmListeners = new ArrayList<>();
    private List<ZomkyLastAppliedListener> zomkyLastAppliedListeners = new ArrayList<>();

    ElectionTimeout electionTimeout;

    Node() {}

    public static Node create(int port, ZomkyStorage zomkyStorage, List<Integer> clientPorts, StateMachine stateMachine, ElectionTimeout electionTimeout) {
        Node node = new Node(port, zomkyStorage);
        node.receiver = new Receiver(node);
        node.senders = new Senders(node, clientPorts);
        node.stateMachine = stateMachine;
        node.electionTimeout = electionTimeout;
        LOGGER.info("[Node {}] has been initialized", node);
        return node;
    }

    public static Node create(int port, ZomkyStorage zomkyStorage, StateMachine stateMachine, List<Integer> clientPorts) {
        return create(port, zomkyStorage, clientPorts, stateMachine, new ElectionTimeout());
    }

    private Node(int port, ZomkyStorage zomkyStorage) {
        this.nodeId = port;
        this.zomkyStorage = zomkyStorage;
    }

    public void start() {
        receiver.start();
        senders.start();
        if (stateMachine != null) {
            stateMachineExecutor.scheduleWithFixedDelay(() -> {
                while (lastApplied.get() < commitIndex.get()) {
                    ByteBuffer response = stateMachine.applyLogEntry(zomkyStorage.getEntryByIndex(lastApplied.incrementAndGet()));
                    zomkyLastAppliedListeners.forEach(zomkyLastAppliedListener -> zomkyLastAppliedListener.handle(lastApplied.get(), Unpooled.wrappedBuffer(response)));
                }
            }, 0, 10, TimeUnit.MILLISECONDS);
        }
        nodeState.onInit(this, zomkyStorage);
    }

    public void stop() {
        nodeState.onExit(this, zomkyStorage);
        receiver.stop();
        senders.stop();
        stateMachineExecutor.shutdownNow();
    }

    Flux<Sender> availableSenders() {
        return senders.availableSenders();
    }

    Mono<Payload> onClientRequest(Payload payload) {
        return nodeState.onClientRequest(this, zomkyStorage, payload);
    }

    Flux<Payload> onClientRequests(Publisher<Payload> payloads) {
        return nodeState.onClientRequests(this, zomkyStorage, payloads);
    }

    void voteForMyself() {
        int term = zomkyStorage.getTerm();
        zomkyStorage.update(term + 1, nodeId);
    }

    boolean notVoted(long term) {
        return term > zomkyStorage.getTerm() || (term == zomkyStorage.getTerm() && zomkyStorage.getVotedFor() == 0);
    }

    public long getCommitIndex() {
        return commitIndex.get();
    }

    public void setCommitIndex(long commitIndex) {
        LOGGER.debug("[Node {}] Set new commit index to {}", nodeId, commitIndex);
        this.commitIndex.set(commitIndex);
        zomkyConfirmListeners.forEach(zomkyStorageConfirmListener -> zomkyStorageConfirmListener.handle(commitIndex));
    }

    public void addConfirmListener(ZomkyConfirmListener zomkyStorageConfirmListener) {
        zomkyConfirmListeners.add(zomkyStorageConfirmListener);
    }

    public void addLastAppliedListener(ZomkyLastAppliedListener zomkyLastAppliedListener) {
        zomkyLastAppliedListeners.add(zomkyLastAppliedListener);
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
        return nodeState.onAppendEntries(this, zomkyStorage, appendEntries)
                .doOnNext(response -> setCurrentLeader(appendEntries.getLeaderId()))
                .doOnNext(response -> LOGGER.debug("[Node {} -> Node {}] Append entries {} -> {}", appendEntries.getLeaderId(), nodeId, appendEntries, response));
    }

    Mono<VoteResponse> onRequestVote(VoteRequest requestVote) {
        return nodeState.onRequestVote(this, zomkyStorage, requestVote)
                .doOnNext(voteResponse -> LOGGER.info("[Node {} -> Node {}] Vote {} -> {}", requestVote.getCandidateId(), nodeId, requestVote, voteResponse));
    }

    void convertToFollower(int term) {
        if (term < zomkyStorage.getTerm()) {
            throw new RaftException(String.format("[Node %s] [current state %s] Term can only be increased! Current term %s vs %s.", nodeId, nodeState, zomkyStorage.getTerm(), term));
        }
        if (term > zomkyStorage.getTerm()) {
            zomkyStorage.update(term, 0);
        }
        convertToFollower();
    }

    void convertToFollower() {
        transitionBetweenStates(this.nodeState, NodeState.FOLLOWER);
    }

    void convertToCandidate() {
        resetCurrentLeader();
        transitionBetweenStates(NodeState.FOLLOWER, NodeState.CANDIDATE);
    }

    void convertToLeader() {
        transitionBetweenStates(NodeState.CANDIDATE, NodeState.LEADER);
        setCurrentLeader(nodeId);
    }

    private void transitionBetweenStates(NodeState stateFrom, NodeState stateTo) {
        if (nodeState != stateFrom) {
            throw new RaftException(String.format("[Node %s] [current state %s] Cannot transition from %s to %s.", nodeId, nodeState, stateFrom, stateTo));
        }
        LOGGER.info("[Node {}] State transition {} -> {}", nodeId, stateFrom, stateTo);
        nodeState.onExit(this, zomkyStorage);
        nodeState = stateTo;
        nodeState.onInit(this, zomkyStorage);
    }

    public int getCurrentLeaderId() {
        return currentLeaderId.get();
    }

    @Override
    public String toString() {
        return "Node{" +
                "nodeState=" + nodeState +
                ", nodeId=" + nodeId +
                '}';
    }
}
