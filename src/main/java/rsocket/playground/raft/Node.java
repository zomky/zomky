package rsocket.playground.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rsocket.playground.raft.h2.H2;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class Node {

    private static final Logger LOGGER = LoggerFactory.getLogger(Node.class);

    volatile NodeState nodeState = NodeState.FOLLOWER;
    int nodeId;

    private AtomicInteger currentLeaderId = new AtomicInteger(0);
    private volatile long currentTerm;
    private volatile Integer votedFor;

    /**
     * index of highest log entry known to be
     * committed (initialized to 0, increases
     * monotonically)
     */
    private volatile long commitIndex;

    /**
     * index of highest log entry applied to state
     * machine (initialized to 0, increases
     * monotonically)
     */
    private volatile long lastApplied;

    /**
     * for each server, index of the next log entry
     * to send to that server (initialized to leader
     * last log index + 1)
     */
    private long nextIndex[];

    /**
     * for each server, index of highest log entry
     * known to be replicated on server
     * (initialized to 0, increases monotonically)
     */
    private long matchIndex[];

    private Receiver receiver;
    private Senders senders;
    private NodeRepository nodeRepository;

    private Set<SenderAvailableCallback> senderAvailableCallbacks = new HashSet<>();
    private Set<SenderUnavailableCallback> senderUnavailableCallbacks = new HashSet<>();

    public static Node create(int port, NodeRepository nodeRepository, List<Integer> clientPorts) {
        Node node = new Node(port, nodeRepository);
        node.receiver = new Receiver(node);
        node.senders = new Senders(node, clientPorts);
        LOGGER.info("[Node {}] has been initialized", node);
        return node;
    }

    private Node(int port, NodeRepository nodeRepository) {
        this.nodeId = port;
        this.currentTerm = getNodeData().getCurrentTerm();
        this.nodeRepository = nodeRepository;
    }

    public void start() {
        receiver.start();
        senders.start();
        nodeState.onInit(this);
    }

    public void stop() {
        nodeState.onExit(this);
        receiver.stop();
        senders.stop();
    }

    Flux<Sender> availableSenders() {
        return senders.availableSenders();
    }

    void voteForMyself() {
        nodeRepository.voteForMyself(nodeId);
        currentTerm = currentTerm + 1; // TODO non-atomic operation
        votedFor = nodeId;
    }

    void setCurrentTerm(long term) {
        assert term > currentTerm;
        H2.updateTerm(nodeId, term);
        currentTerm = term;
        votedFor = null;
    }

    void voteForCandidate(int candidateId, long term) {
        assert term >= currentTerm;
        nodeRepository.voteForCandidate(nodeId, candidateId, term);
        currentTerm = term;
        votedFor = candidateId;
    }

    boolean notVoted(long term) {
        return term > currentTerm || (term == currentTerm && votedFor == null);
    }

    NodeData getNodeData() {
        return H2.nodeDataFromDB(nodeId).orElseThrow(() -> new RaftException("no nodeData"));
    }

    long getCurrentTerm() {
        return currentTerm;
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
        return nodeState.onAppendEntries(this, appendEntries)
                .doOnNext(response -> setCurrentLeader(appendEntries.getLeaderId()))
                .doOnNext(response -> LOGGER.debug("[Node {} -> Node {}] Append entries {} -> {}", appendEntries.getLeaderId(), nodeId, appendEntries, response));
    }

    Mono<VoteResponse> onRequestVote(VoteRequest requestVote) {
        return nodeState.onRequestVote(this, requestVote)
                .doOnNext(voteResponse -> LOGGER.info("[Node {} -> Node {}] Vote {} -> {}", requestVote.getCandidateId(), nodeId, requestVote, voteResponse));
    }

    void convertToFollower(long term) {
        if (term < currentTerm) {
            throw new RaftException(String.format("[Node %s] [current state %s] Term can only be increased! Current term %s vs %s.", nodeId, nodeState, currentTerm, term));
        }
        if (term > currentTerm) {
            setCurrentTerm(term);
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
        nodeState.onExit(this);
        nodeState = stateTo;
        nodeState.onInit(this);
    }

    @Override
    public String toString() {
        return "Node{" +
                "nodeState=" + nodeState +
                ", nodeId=" + nodeId +
                ", currentTerm=" + currentTerm +
                '}';
    }
}
