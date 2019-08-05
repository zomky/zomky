package io.github.pmackowski.rsocket.raft.gossip;

import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ack;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Gossip;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ping;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.List;

class GossipProbe {

    private static final Logger LOGGER = LoggerFactory.getLogger(GossipProbe.class);

    private int nodeId;
    private GossipTransport gossipTransport;

    public GossipProbe(GossipNode node) {
        this.nodeId = node.nodeId;
        this.gossipTransport = new GossipTransport();
    }

    public Mono<Collection<? super Ack>> probeNode(int destinationNodeId, List<Integer> proxyNodeIds, List<Gossip> gossips, Publisher<?> indirectStart, Publisher<?> protocolPeriodEnd) {
        return pingDirect(destinationNodeId, gossips)
                .transform(pingDirect -> new ProbeOperator<>(pingDirect, pingIndirect(destinationNodeId, proxyNodeIds, gossips), indirectStart, protocolPeriodEnd))
                .doOnSubscribe(subscription -> LOGGER.info("[Node {}][ping] Probing {} ...", this.nodeId, destinationNodeId));
    }

    private Mono<Ack> pingDirect(int destinationNodeId, List<Gossip> gossips) {
        return gossipTransport.ping(Ping.newBuilder()
                .setInitiatorNodeId(this.nodeId)
                .setRequestorNodeId(this.nodeId)
                .setDestinationNodeId(destinationNodeId)
                .addAllGossips(gossips)
                .setDirect(true)
                .build());
    }

    private Flux<Ack> pingIndirect(int destinationNodeId, List<Integer> proxies, List<Gossip> gossips) {
        return Flux.fromIterable(proxies)
                .flatMap(proxyNodeId -> gossipTransport.ping(Ping.newBuilder()
                        .setInitiatorNodeId(this.nodeId)
                        .setRequestorNodeId(proxyNodeId)
                        .setDestinationNodeId(destinationNodeId)
                        .addAllGossips(gossips)
                        .setDirect(false)
                        .build())
                );
    }

}
