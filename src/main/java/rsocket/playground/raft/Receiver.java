package rsocket.playground.raft;

import io.rsocket.*;
import io.rsocket.transport.netty.server.TcpServerTransport;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rsocket.playground.raft.transport.ObjectPayload;

public class Receiver {

    private Node node;
    private Disposable disposable;

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
    }

    public void stop() {
        disposable.dispose();
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
