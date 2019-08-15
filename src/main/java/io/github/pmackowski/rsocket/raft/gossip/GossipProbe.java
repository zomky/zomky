package io.github.pmackowski.rsocket.raft.gossip;

import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ack;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Gossip;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ping;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

class GossipProbe {

    private static final Logger LOGGER = LoggerFactory.getLogger(GossipProbe.class);

    private int nodeId;
    private GossipTransport gossipTransport;
    private Map<Integer, AtomicLong> clock = new ConcurrentHashMap<>();

    GossipProbe(int nodeId, GossipTransport gossipTransport) {
        this.nodeId = nodeId;
        this.gossipTransport = gossipTransport;
    }

    Mono<ProbeResult> probeNode(PeerProbe peerProbe, List<Gossip> hotGossips, Duration indirectDelay, Duration probeTimeout) {
        Integer destinationNodeId = peerProbe.getDestinationNodeId();
        if (destinationNodeId == null) {
            return Mono.delay(probeTimeout).thenReturn(ProbeResult.NO_PROBE_ACKS);
        }

        Publisher<?> indirectDelayPublisher = Mono.delay(indirectDelay);
        Publisher<?> probeTimeoutPublisher = Mono.delay(probeTimeout);

        List<Integer> proxyNodeIds = peerProbe.getProxyNodeIds();
        return new ProbeOperator<>(pingDirect(destinationNodeId, hotGossips), pingIndirect(destinationNodeId, proxyNodeIds, hotGossips), indirectDelayPublisher, probeTimeoutPublisher)
                .map(probeOperatorResult -> ProbeResult.builder()
                        .destinationNodeId(destinationNodeId)
                        .probeResult(probeOperatorResult)
                        .subgroupSize(probeOperatorResult.isIndirect() ? peerProbe.getSubgroupSize() : 0)
                        .missedNack(missedNack(probeOperatorResult, peerProbe))
                        .hotGossips(hotGossips)
                        .build()
                );
    }

    private boolean missedNack(ProbeOperatorResult<Ack> probeOperatorResult, PeerProbe peerProbe) {
        if (!probeOperatorResult.isIndirect()) {
            return false;
        }
        int expectedIndirectAckOrNack =  peerProbe.getSubgroupSize() +
                (probeOperatorResult.isDirectSuccessful() ? 1 : 0);

        return probeOperatorResult.getElements().size() != expectedIndirectAckOrNack;
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
                .doOnNext(ack -> log(ping))
                .doOnError(throwable -> logError(ping, throwable));
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
                            // TODO (probeTimeout - indirectDelay ) * 0.8f ??
                            // setNackTimeout ???
                            //.setIndirectPingTimeout()
                            .build();
                    return gossipTransport
                            .ping(ping)
                            .doOnNext(ack -> log(ping))
                            .onErrorResume(throwable -> {
                                // cannot connect to proxy
                                logError(ping, throwable);
                                return Mono.empty();
                            });
                });
    }

    private void log(Ping ping) {
        if (ping.getDirect()) {
            LOGGER.info("[Node {}][ping] Direct probe to {} successful.", ping.getInitiatorNodeId(), ping.getDestinationNodeId());
        } else {
            LOGGER.info("[Node {}][ping] Indirect probe to {} through {} successful.", ping.getInitiatorNodeId(), ping.getDestinationNodeId(), ping.getRequestorNodeId());
        }
    }

    private void logError(Ping ping, Throwable throwable) {
        if (ping.getDirect()) {
            LOGGER.warn("[Node {}][ping] Direct probe to {} failed. Reason {}.", ping.getInitiatorNodeId(), ping.getDestinationNodeId(), throwable.getMessage());
        } else {
            LOGGER.warn("[Node {}][ping] Indirect probe to {} through {} failed. Reason {}", ping.getInitiatorNodeId(), ping.getDestinationNodeId(), ping.getRequestorNodeId(), throwable.getMessage());
        }
    }

}
