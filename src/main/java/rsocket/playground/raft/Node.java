package rsocket.playground.raft;

import io.rsocket.Payload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rsocket.playground.raft.storage.ZomkyStorage;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class Node {

    private static final Logger LOGGER = LoggerFactory.getLogger(Node.class);

    volatile NodeState nodeState = NodeState.FOLLOWER;
    int nodeId;

    private AtomicInteger currentLeaderId = new AtomicInteger(0);

    /**
     * index of highest log entry known to be
     * committed (initialized to 0, increases
     * monotonically)
     */
    private volatile long commitIndex = 0;

    /**
     * index of highest log entry applied to state
     * machine (initialized to 0, increases
     * monotonically)
     */
    private volatile long lastApplied = 0;

    private Receiver receiver;
    private Senders senders;
    private ZomkyStorage zomkyStorage;

    private Set<SenderAvailableCallback> senderAvailableCallbacks = new HashSet<>();
    private Set<SenderUnavailableCallback> senderUnavailableCallbacks = new HashSet<>();

    ElectionTimeout electionTimeout;

    public static Node create(int port, ZomkyStorage zomkyStorage, List<Integer> clientPorts, ElectionTimeout electionTimeout) {
        Node node = new Node(port, zomkyStorage);
        node.receiver = new Receiver(node);
        node.senders = new Senders(node, clientPorts);
        node.electionTimeout = electionTimeout;
        LOGGER.info("[Node {}] has been initialized", node);
        return node;
    }

    public static Node create(int port, ZomkyStorage zomkyStorage, List<Integer> clientPorts) {
        return create(port, zomkyStorage, clientPorts, new ElectionTimeout());
    }

    private Node(int port, ZomkyStorage zomkyStorage) {
        this.nodeId = port;
        this.zomkyStorage = zomkyStorage;
    }

    public void start() {
        receiver.start();
        senders.start();
        nodeState.onInit(this, zomkyStorage);
    }

    public void stop() {
        nodeState.onExit(this, zomkyStorage);
        receiver.stop();
        senders.stop();
    }

    Flux<Sender> availableSenders() {
        return senders.availableSenders();
    }

    Mono<Payload> onClientRequest(Payload payload) {
        return nodeState.onClientRequest(this, zomkyStorage, payload);
    }

    void voteForMyself() {
        int term = zomkyStorage.getTerm();
        zomkyStorage.update(term + 1, nodeId);
    }

    boolean notVoted(long term) {
        return term > zomkyStorage.getTerm() || (term == zomkyStorage.getTerm() && zomkyStorage.getVotedFor() == 0);
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        LOGGER.info("[Node {}] Set new commit index to {}", nodeId, commitIndex);
        this.commitIndex = commitIndex;
    }

    public void increaseCommitIndex() {
        commitIndex = commitIndex + 1;
    }

    public long getLastApplied() {
        return lastApplied;
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
