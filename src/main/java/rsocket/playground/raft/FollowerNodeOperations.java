package rsocket.playground.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.time.Duration;

public class FollowerNodeOperations implements NodeOperations{

    private static final Logger LOGGER = LoggerFactory.getLogger(FollowerNodeOperations.class);

    @Override
    public Mono<Void> onInit(Node node) {
        return node.processor.timeout(Duration.ofMillis(300))
                .onErrorResume(throwable -> {
                    return node.makeCandidate();

                });

    }

    @Override
    public Mono<Void> onExit(Node node) {
        return null;
    }
}
