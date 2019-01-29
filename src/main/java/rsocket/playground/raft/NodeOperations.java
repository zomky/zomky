package rsocket.playground.raft;

import reactor.core.publisher.Mono;

public interface NodeOperations {

//    Flux<Payload> requestChannel(Publisher<Payload> payloads);

    Mono<Void> onInit(Node node);

    Mono<Void> onExit(Node node);

}
