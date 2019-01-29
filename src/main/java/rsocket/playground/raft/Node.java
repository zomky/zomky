package rsocket.playground.raft;

import io.rsocket.*;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import rsocket.playground.raft.transport.ObjectPayload;

import java.util.List;

public class Node {

    private static final Logger LOGGER = LoggerFactory.getLogger(Node.class);

    NodeState nodeState;
    NodeData nodeData;
    int nodeId;

    Mono<CloseableChannel> receiver;
    Flux<RSocket> senders;

    public Node(int port, List<Integer> clientPorts) {
        this.nodeId = port;
        // TODO node data should be read from persistent storage
        this.nodeData = new NodeData().currentTerm(0);
        this.nodeState = NodeState.FOLLOWER;
        this.receiver = RSocketFactory.receive()
                .acceptor(new SocketAcceptorImpl(this))
                .transport(TcpServerTransport.create("localhost", port))
                .start();

        this.senders = Flux.fromIterable(clientPorts)
               .flatMap(clientPort -> RSocketFactory.connect()
                       .transport(TcpClientTransport.create("localhost", clientPort))
                       .start())
               .cache();
    }

    public void start() {
        nodeState.onInit(this);
        receiver.subscribe();
    }

    void increaseCurrentTerm() {
        this.nodeData.currentTerm(nodeData.getCurrentTerm() + 1);
    }

    long getCurrentTerm() {
        return nodeData.getCurrentTerm();
    }

    private Mono<Payload> onPayloadRequest(Payload payload) {
        return nodeState.onPayloadRequest(payload);
    }

    private Mono<AppendEntriesResult> onAppendEntries(AppendEntries appendEntries) {
        boolean makeFollower = isGreaterThanCurrentTerm(appendEntries.getTerm());

        return makeFollower(makeFollower)
                .then(nodeState.onAppendEntries(this, appendEntries));
    }

    private Mono<RequestVoteResult> onRequestVote(RequestVote requestVote) {
        boolean makeFollower = isGreaterThanCurrentTerm(requestVote.getTerm());

        return makeFollower(makeFollower)
                .then(nodeState.onRequestVote(this, requestVote));
    }

    public Mono<Void> makeFollower(boolean makeFollower) {
        return Mono.empty();
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

    boolean isGreaterThanCurrentTerm(long term) {
        return getCurrentTerm() < term;
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
                               .flatMap(node::onPayloadRequest)
                               .flatMap(payload -> {
                                   AppendEntries appendEntries = ObjectPayload.dataFromPayload(payload, AppendEntries.class);
                                   return node.onAppendEntries(appendEntries);
                               })
                               .map(ObjectPayload::create);
                }

                @Override
                public Mono<Payload> requestResponse(Payload payload) {
                    // request vote
                    return node.onPayloadRequest(payload)
                                .flatMap(payload1 -> {
                                    RequestVote requestVote = ObjectPayload.dataFromPayload(payload1, RequestVote.class);
                                    return node.onRequestVote(requestVote);
                                })
                                .map(ObjectPayload::create);
                }
            });
        }

    }

}
