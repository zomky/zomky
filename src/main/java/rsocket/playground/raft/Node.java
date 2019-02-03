package rsocket.playground.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rsocket.playground.raft.h2.H2;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Node {

    private static final Logger LOGGER = LoggerFactory.getLogger(Node.class);

    private NodeState nodeState = NodeState.FOLLOWER;
    int nodeId;

    private long currentTerm;
    private Integer votedFor;

    private Receiver receiver;
    private Senders senders;
    private NodeRepository nodeRepository;

    private Set<SenderAvailableCallback> senderAvailableCallbacks = new HashSet<>();
    private Set<SenderUnavailableCallback> senderUnavailableCallbacks = new HashSet<>();

    public static Node create(int port, NodeRepository nodeRepository, List<Integer> clientPorts) {
        Node node = new Node(port, nodeRepository);
        node.receiver = new Receiver(node);
        node.senders = new Senders(node, clientPorts);
        LOGGER.info("Node {} finished", node);
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
        currentTerm++;
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

    Mono<AppendEntriesResponse> onAppendEntries(AppendEntriesRequest appendEntries) {
        return nodeState.onAppendEntries(this, appendEntries)
                .doOnNext(response -> LOGGER.debug("Node {}. Append entries {}, {}", nodeId, appendEntries, response));
    }

    Mono<VoteResponse> onRequestVote(VoteRequest requestVote) {
        return nodeState.onRequestVote(this, requestVote)
                .doOnNext(voteResponse -> LOGGER.info("Node {}. Vote {} , {}", nodeId, requestVote, voteResponse));
    }

    void convertToFollower(long term) {
        assert term >= currentTerm;
        if (term > currentTerm) {
            setCurrentTerm(term);
        }
        convertToFollower();
    }

    void convertToFollower() {
        transitionBetweenStates(this.nodeState, NodeState.FOLLOWER);
    }

    void convertToCandidate() {
        transitionBetweenStates(NodeState.FOLLOWER, NodeState.CANDIDATE);
    }

    void convertToLeader() {
        transitionBetweenStates(NodeState.CANDIDATE, NodeState.LEADER);
    }

    private void transitionBetweenStates(NodeState stateFrom, NodeState stateTo) {
        LOGGER.info("Node {}. State transition {} -> {}", nodeId, stateFrom, stateTo);
        assert nodeState == stateFrom;
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
