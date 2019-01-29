package rsocket.playground.raft;

import reactor.core.publisher.Mono;

public class LeaderNodeOperations implements NodeOperations{

    @Override
    public Mono<Void> onInit(Node node) {
        return null;
    }

    @Override
    public Mono<Void> onExit(Node node) {
        return null;
    }
}
