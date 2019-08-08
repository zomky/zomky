package io.github.pmackowski.rsocket.raft.gossip;

import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ack;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Gossip;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ping;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static io.github.pmackowski.rsocket.raft.gossip.GossipLogger.log;
import static io.github.pmackowski.rsocket.raft.gossip.GossipLogger.logError;

class GossipProbe {

    private int nodeId;
    private GossipTransport gossipTransport;
    private Map<Integer, AtomicLong> clock = new ConcurrentHashMap<>();

    GossipProbe(int nodeId, GossipTransport gossipTransport) {
        this.nodeId = nodeId;
        this.gossipTransport = gossipTransport;
    }

    Mono<Acks> probeNode(PeerProbe peerProbe, List<Gossip> gossips, Publisher<?> indirectStart, Publisher<?> protocolPeriodEnd) {
        Integer destinationNodeId = peerProbe.getDestinationNodeId();
        if (destinationNodeId == null) {
            return Mono.from(protocolPeriodEnd).thenReturn(Acks.NO_ACKS);
        }
        List<Integer> proxyNodeIds = peerProbe.getProxyNodeIds();
        return pingDirect(destinationNodeId, gossips)
                .transform(pingDirect -> new ProbeOperator<>(pingDirect, pingIndirect(destinationNodeId, proxyNodeIds, gossips), indirectStart, protocolPeriodEnd))
                .map(acks -> new Acks(destinationNodeId, acks));
    }

    private Mono<Ack> pingDirect(int destinationNodeId, List<Gossip> gossips) {
        Ping ping = Ping.newBuilder()
                .setInitiatorNodeId(this.nodeId)
                .setRequestorNodeId(this.nodeId)
                .setDestinationNodeId(destinationNodeId)
                .addAllGossips(gossips)
                .setDirect(true)
                .setCounter(clock.getOrDefault(destinationNodeId, new AtomicLong(0)).incrementAndGet())
                .build();
        return gossipTransport
                .ping(ping)
                .doOnNext(i -> log(ping))
                .onErrorResume(throwable -> {
                    logError(ping, throwable);
                    return Mono.empty();
                });
    }

    private Flux<Ack> pingIndirect(int destinationNodeId, List<Integer> proxies, List<Gossip> gossips) {
        return Flux.fromIterable(proxies)
                .flatMap(proxyNodeId -> {
                    Ping ping = Ping.newBuilder()
                            .setInitiatorNodeId(this.nodeId)
                            .setRequestorNodeId(proxyNodeId)
                            .setDestinationNodeId(destinationNodeId)
                            .addAllGossips(gossips)
                            .setDirect(false)
                            .build();
                    return gossipTransport
                            .ping(ping)
                            .doOnNext(i -> log(ping))
                            .onErrorResume(throwable -> {
                                logError(ping, throwable);
                                return Mono.empty();
                            });
                });
    }

}
