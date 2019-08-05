package io.github.pmackowski.rsocket.raft.gossip;

import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.function.BiFunction;

public interface GossipOnPingDelay extends BiFunction<Integer,Long,Mono<Long>> {

    GossipOnPingDelay NO_DELAY = (nodeId, protocolPeriodCounter) -> Mono.delay(Duration.ZERO);

}
