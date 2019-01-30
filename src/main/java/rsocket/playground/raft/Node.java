package rsocket.playground.raft;

import io.rsocket.*;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rsocket.playground.raft.h2.H2;
import rsocket.playground.raft.transport.ObjectPayload;

import java.util.List;

public class Node {

    // TODO node configuration
    private static final String BIND_ADDRESS = "localhost";

    private static final Logger LOGGER = LoggerFactory.getLogger(Node.class);

    NodeState nodeState;
    int nodeId;
    long currentTerm;

    Mono<CloseableChannel> receiver;
    Flux<RSocket> senders;

    public Node(int port, List<Integer> clientPorts) {
        this.nodeId = port;
        this.nodeState = NodeState.FOLLOWER;
        this.currentTerm = getNodeData().getCurrentTerm();

        this.receiver = RSocketFactory.receive()
                .acceptor(new SocketAcceptorImpl(this))
                .transport(TcpServerTransport.create(BIND_ADDRESS, port))
                .start();

        this.senders = Flux.fromIterable(clientPorts)
               .flatMap(clientPort -> RSocketFactory.connect()
                       .transport(TcpClientTransport.create(BIND_ADDRESS, clientPort))
                       .start());
    }

    public void start() {
        nodeState.onInit(this);
        receiver.subscribe();
    }

    void increaseCurrentTerm() {
        H2.updateTerm(nodeId);
        currentTerm++;
    }

    void setCurrentTerm(long term) {
        H2.updateTerm(nodeId, term);
        currentTerm = term;
    }

    NodeData getNodeData() {
        return H2.nodeDataFromDB(nodeId).orElseThrow(() -> new RaftException("no nodeData"));
    }

    long getCurrentTerm() {
        return currentTerm;
    }

    private Mono<AppendEntriesResponse> onAppendEntries(AppendEntriesRequest appendEntries) {
        return nodeState.onAppendEntries(this, appendEntries);
    }

    private Mono<VoteResponse> onRequestVote(VoteRequest requestVote) {
        return nodeState.onRequestVote(this, requestVote);
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

    private static class SocketAcceptorImpl implements SocketAcceptor {

        private Node node;

        public SocketAcceptorImpl(Node node) {
            this.node = node;
        }

        @Override
        public Mono<RSocket> accept(ConnectionSetupPayload setupPayload, RSocket reactiveSocket) {
            return Mono.just(new AbstractRSocket() {

                @Override
                public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                    // append entries
                    return Flux.from(payloads)
                               .flatMap(payload -> {
                                   AppendEntriesRequest appendEntries = ObjectPayload.dataFromPayload(payload, AppendEntriesRequest.class);
                                   return node.onAppendEntries(appendEntries)
                                           .doOnNext(voteResponse -> node.convertToFollowerIfObsolete(appendEntries.getTerm()));
                               })
                               .map(ObjectPayload::create);
                }

                @Override
                public Mono<Payload> requestResponse(Payload payload) {
                    // request vote
                    return Mono.just(payload)
                                .flatMap(payload1 -> {
                                    VoteRequest voteRequest = ObjectPayload.dataFromPayload(payload1, VoteRequest.class);
                                    return node.onRequestVote(voteRequest)
                                               .doOnNext(voteResponse -> node.convertToFollowerIfObsolete(voteRequest.getTerm()));
                                }).map(ObjectPayload::create);
                }
            });
        }

    }

}
