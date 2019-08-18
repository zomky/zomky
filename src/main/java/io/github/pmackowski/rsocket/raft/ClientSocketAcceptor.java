package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.raft.RaftProtocol;
import io.rsocket.*;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ClientSocketAcceptor implements SocketAcceptor {

    private RaftProtocol raftProtocol;

    public ClientSocketAcceptor(RaftProtocol raftProtocol) {
        this.raftProtocol = raftProtocol;
    }

    @Override
    public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
        return Mono.just(new AbstractRSocket() {

            @Override
            public Mono<Payload> requestResponse(Payload payload) {
                return raftProtocol.onClientRequest(payload);
            }

            @Override
            public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                return raftProtocol.onClientRequests(payloads);
            }

        });
    }

}
