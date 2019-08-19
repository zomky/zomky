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

    Mono<ProbeResult> probeNode(PeerProbe peerProbe, List<Gossip> hotGossips, PeerProbeTimeouts peerProbeTimeouts) {
        Integer destinationNodeId = peerProbe.getDestinationNodeId();
        if (destinationNodeId == null) {
            return Mono.delay(peerProbeTimeouts.probeTimeout()).then(Mono.empty());
        }

        Publisher<?> indirectDelayPublisher = Mono.delay(peerProbeTimeouts.indirectDelay());
        Publisher<?> probeTimeoutPublisher = Mono.delay(peerProbeTimeouts.probeTimeout());

        List<Integer> proxyNodeIds = peerProbe.getProxyNodeIds();
        return new ProbeOperator<>(pingDirect(destinationNodeId, hotGossips),
                                   pingIndirect(destinationNodeId, proxyNodeIds, peerProbeTimeouts.nackTimeout(), hotGossips),
                                   indirectDelayPublisher,
                                   probeTimeoutPublisher)
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
        int expectedAckOrNack =  peerProbe.getSubgroupSize() +
                (probeOperatorResult.isDirectSuccessful() ? 1 : 0);

        long ackOrNack = probeOperatorResult.getElements().stream()
                .map(Ack::getNodeId)
                .distinct()
                .count();
        return ackOrNack != expectedAckOrNack;
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
                .next()
                .doOnNext(ack -> log(ping, ack))
                .doOnError(throwable -> {
                    LOGGER.warn("[Node {}][ping] Direct probe to {} failed. Reason {}.", ping.getInitiatorNodeId(), ping.getDestinationNodeId(), throwable.getMessage());
                });
    }

    private Flux<Ack> pingIndirect(int destinationNodeId, List<Integer> proxies, Duration nackTimeout, List<Gossip> gossips) {
        return Flux.fromIterable(proxies)
                .flatMap(proxyNodeId -> {
                    Ping ping = Ping.newBuilder()
                            .setInitiatorNodeId(this.nodeId)
                            .setRequestorNodeId(proxyNodeId)
                            .setDestinationNodeId(destinationNodeId)
                            .addAllGossips(gossips)
                            .setDirect(false)
                            .setNackTimeout((int) nackTimeout.toMillis())
                            .build();
                    return gossipTransport
                            .ping(ping)
                            .doOnNext(ack -> log(ping, ack))
                            .onErrorResume(throwable -> {
                                // cannot connect to proxy
                                LOGGER.warn("[Node {}][ping] Indirect probe to {} through {} failed. Reason {}", ping.getInitiatorNodeId(), ping.getDestinationNodeId(), ping.getRequestorNodeId(), throwable.getMessage());
                                return Mono.empty();
                            });
                });
    }

    private void log(Ping ping, Ack ack) {
        if (ping.getDirect()) {
            LOGGER.info("[Node {}][ping] Direct probe to {} successful.", ping.getInitiatorNodeId(), ping.getDestinationNodeId());
        } else {
            if (ack.getNack()) {
                LOGGER.info("[Node {}][ping] Indirect probe to {} through {} failed.", ping.getInitiatorNodeId(), ping.getDestinationNodeId(), ping.getRequestorNodeId());
            } else {
                LOGGER.info("[Node {}][ping] Indirect probe to {} through {} successful.", ping.getInitiatorNodeId(), ping.getDestinationNodeId(), ping.getRequestorNodeId());
            }
        }
    }

}
