package rsocket.playground.raft;

import io.rsocket.*;
import io.rsocket.transport.netty.server.TcpServerTransport;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rsocket.playground.raft.transport.ObjectPayload;

public class Receiver {

    private static final Logger LOGGER = LoggerFactory.getLogger(Receiver.class);

    private Node node;
    private Disposable disposable, disposable2;

    public Receiver(Node node) {
        this.node = node;
    }

    public void start() {
        disposable = RSocketFactory.receive()
                .acceptor(new SocketAcceptorImpl(node))
                .transport(TcpServerTransport.create(node.nodeId))
                .start()
                .block()
                .onClose()
                .subscribe();

        disposable2 = RSocketFactory.receive()
                .acceptor(new ClientSocketAcceptor(node))
                .transport(TcpServerTransport.create(node.nodeId + 10000))
                .start()
                .block()
                .onClose()
                .subscribe();
    }

    public void stop() {
        disposable.dispose();
        disposable2.dispose();
    }

    private static class ClientSocketAcceptor implements SocketAcceptor {

        private Node node;

        public ClientSocketAcceptor(Node node) {
            this.node = node;
        }

        @Override
        public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
            return Mono.just(new AbstractRSocket() {

                @Override
                public Mono<Payload> requestResponse(Payload payload) {
                    // 0. redirect to leader
                    // 1. append to the leaders log
                    // 2. send to the followers (on the next heartbeat)
                    // 3. commit an entry once a majority of followers acknowledge it
                    // 4. respond to the client
                    return Mono.just(payload)
                        .doOnNext(s -> LOGGER.info("Server received payload"));
                }
            });
        }
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
                            .map(payload -> ObjectPayload.dataFromPayload(payload, AppendEntriesRequest.class))
                            .flatMap(appendEntriesRequest -> node.onAppendEntries(appendEntriesRequest))
                            .map(ObjectPayload::create);
                }

                @Override
                public Mono<Payload> requestResponse(Payload payload) {
                    // request vote
                    return Mono.just(payload)
                            .map(payload1 -> ObjectPayload.dataFromPayload(payload1, VoteRequest.class))
                            .flatMap(voteRequest -> node.onRequestVote(voteRequest))
                            .map(ObjectPayload::create);
                }
            });
        }

    }
}
