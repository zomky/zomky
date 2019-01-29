package rsocket.playground.raft;

import io.rsocket.*;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

public class Node {

    private static final Logger LOGGER = LoggerFactory.getLogger(Node.class);

    NodeState nodeState = NodeState.FOLLOWER;
    Flux<RSocket> otherServers;
    DirectProcessor<Payload> processor;
    FluxSink<Payload> sink;
    Disposable disposable;

    public Node() {
        disposable = RSocketFactory.receive()
                .acceptor(new SocketAcceptorImpl(this))
                .transport(TcpServerTransport.create("localhost", 7000))
                .start()
                .subscribe();

        otherServers = Flux.from(
                RSocketFactory.connect()
                        .transport(TcpClientTransport.create("localhost", 7001))
                        .start()
        );

    }

    public Mono<Void> makeCandidate() {
        assert nodeState == NodeState.FOLLOWER;
        return nodeState.onExit(this)
                        .then(Mono.just(1).doOnNext(i -> nodeState = NodeState.CANDIDATE))
                        .then(nodeState.onInit(this));
    }

    public void makeLeader() {
        assert nodeState == NodeState.CANDIDATE;
        nodeState = NodeState.LEADER;
    }

    public void makeFollower() {

    }

    private static class SocketAcceptorImpl implements SocketAcceptor {

        private Node node;

        public SocketAcceptorImpl(Node node) {
            this.node = node;
        }

        @Override
        public Mono<RSocket> accept(ConnectionSetupPayload setupPayload, RSocket reactiveSocket) {
            return Mono.just(
                    new AbstractRSocket() {
                        @Override
                        public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                            return Flux.from(payloads)
                                       .flatMap(payload -> node.nodeState.requestChannel(payload));
                        }
                    });
        }

    }


}
