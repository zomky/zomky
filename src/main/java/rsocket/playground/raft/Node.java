package rsocket.playground.raft;

import io.rsocket.RSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rsocket.playground.raft.h2.H2;

import java.util.List;

public class Node {

    private static final Logger LOGGER = LoggerFactory.getLogger(Node.class);

    private NodeState nodeState = NodeState.FOLLOWER;
    int nodeId;

    private long currentTerm;
    private Integer votedFor;

    private Receiver receiver;
    private Sender sender;
    private NodeRepository nodeRepository;

    public static Node create(int port, NodeRepository nodeRepository, List<Integer> clientPorts) {
        Node node = new Node(port, nodeRepository);
        node.receiver = new Receiver(node);
        node.sender = new Sender(node, clientPorts);
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
        sender.start();
        nodeState.onInit(this);
    }

    public void stop() {
        nodeState.onExit(this);
        receiver.stop();
        sender.stop();
    }

    Flux<RSocket> availableClients() {
        return sender.senders();
    }

    void voteForMyself() {
        nodeRepository.voteForMyself(nodeId);
        currentTerm++;
    }

    void setCurrentTerm(long term) {
        H2.updateTerm(nodeId, term);
        currentTerm = term;
    }

    void voteForCandidate(int candidateId, long term) {
        nodeRepository.voteForCandidate(nodeId, candidateId, term);
        currentTerm = term;
    }

    NodeData getNodeData() {
        return H2.nodeDataFromDB(nodeId).orElseThrow(() -> new RaftException("no nodeData"));
    }

    long getCurrentTerm() {
        return currentTerm;
    }

    Mono<AppendEntriesResponse> onAppendEntries(AppendEntriesRequest appendEntries) {
        return nodeState.onAppendEntries(this, appendEntries)
                .doOnNext(response -> LOGGER.info("Append entries {}, {}", appendEntries, response));
    }

    Mono<VoteResponse> onRequestVote(VoteRequest requestVote) {
        return nodeState.onRequestVote(this, requestVote)
                .doOnNext(voteResponse -> LOGGER.info("Vote {} , {}", requestVote, voteResponse));
    }

    void convertToFollowerIfObsolete(long term) {
        if (term > getCurrentTerm()) {
            setCurrentTerm(term);
            if (this.nodeState != NodeState.FOLLOWER) {
                convertToFollower();
            }
        }
    }

    void convertToFollower() {
        LOGGER.info("Node {} is being converted to follower", this.nodeId);
        transitionBetweenStates(this.nodeState, NodeState.FOLLOWER);
    }

    void convertToCandidate() {
        LOGGER.info("Node {} is being converted to candidate", this.nodeId);
        transitionBetweenStates(NodeState.FOLLOWER, NodeState.CANDIDATE);
    }

    void convertToLeader() {
        LOGGER.info("Node {} is being converted to leader", this.nodeId);
        transitionBetweenStates(NodeState.CANDIDATE, NodeState.LEADER);
    }

    private void transitionBetweenStates(NodeState stateFrom, NodeState stateTo) {
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
