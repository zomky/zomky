package io.github.pmackowski.rsocket.raft.integration.gossip;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ack;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Gossip;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ping;
import io.github.pmackowski.rsocket.raft.utils.NettyUtils;
import io.netty.channel.socket.DatagramPacket;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.udp.UdpClient;

import java.util.Collection;
import java.util.List;

public class GossipProbe {

    private static final Logger LOGGER = LoggerFactory.getLogger(GossipProbe.class);

    private int nodeId;

    public GossipProbe(GossipNode node) {
        this.nodeId = node.nodeId;
    }

    public Mono<Collection<? super Ack>> probeNode(int destinationNodeId, List<Integer> proxyNodeIds, List<Gossip> gossips, Publisher<?> indirectStart, Publisher<?> protocolPeriodEnd) {
        return pingDirect(destinationNodeId, gossips)
                .transform(pingDirect -> new ProbeOperator<>(pingDirect, pingIndirect(destinationNodeId, proxyNodeIds, gossips), indirectStart, protocolPeriodEnd))
                .doOnSubscribe(subscription -> LOGGER.info("[Node {}][ping] Probing {} ...", this.nodeId, destinationNodeId));
    }

    private Mono<Ack> pingDirect(int destinationNodeId, List<Gossip> gossips) {
        return client(destinationNodeId)
                .flatMap(connection -> {
                    Ping ping = Ping.newBuilder()
                            .setInitiatorNodeId(this.nodeId)
                            .setRequestorNodeId(this.nodeId)
                            .setDestinationNodeId(destinationNodeId)
                            .addAllGossips(gossips)
                            .setDirect(true)
                            .build();

                    return connection.outbound().sendByteArray(Mono.just(ping.toByteArray())).then()
                            .then(connection.inbound().receiveObject().cast(DatagramPacket.class).next()
                                    .doOnNext(i -> LOGGER.info("[Node {}][ping] Direct probe to {} successful.", this.nodeId, destinationNodeId))
                                    .map(this::toAck)
                                    .onErrorResume(throwable -> {
                                        LOGGER.warn("[Node {}][ping] Direct probe to {} failed. Reason {}.", this.nodeId, destinationNodeId, throwable.getMessage());
                                        return Mono.empty();
                                    })
                            );
                });
    }

    private Flux<Ack> pingIndirect(int destinationNodeId, List<Integer> proxies, List<Gossip> gossips) {
        return Flux.fromIterable(proxies)
                .flatMap(proxyNodeId -> pingIndirect(destinationNodeId, proxyNodeId, gossips));
    }

    private Mono<Ack> pingIndirect(int destinationNodeId, int proxyNodeId, List<Gossip> gossips) {
        return client(proxyNodeId)
                .flatMap(connection -> {
                    Ping ping = Ping.newBuilder()
                            .setInitiatorNodeId(this.nodeId)
                            .setRequestorNodeId(proxyNodeId)
                            .setDestinationNodeId(destinationNodeId)
                            .addAllGossips(gossips)
                            .setDirect(false)
                            .build();

                    return connection.outbound().sendByteArray(Mono.just(ping.toByteArray())).then()
                            .then(connection.inbound().receiveObject().cast(DatagramPacket.class).next()
                                    .doOnNext(i -> LOGGER.info("[Node {}][ping] Indirect probe to {} through {} successful.", this.nodeId, destinationNodeId, proxyNodeId))
                                    .map(this::toAck)
                                    .onErrorResume(throwable -> {
                                        LOGGER.warn("[Node {}][ping] Indirect probe to {} through {} failed. Reason {}.", this.nodeId, destinationNodeId, proxyNodeId, throwable.getMessage());
                                        return Mono.empty();
                                    })
                            );
                });
    }

    private Mono<? extends Connection> client(int port) {
        return UdpClient.create()
                .port(port)
//                .runOn(loopResources)
                .connect()
                .doOnSubscribe(subscription -> {
                    LOGGER.info("[Node {}] Probe {} subscription", nodeId, port);
                })
                .doOnCancel(() -> {
                    LOGGER.info("[Node {}] Indirect probe {} cancelled", nodeId, port);
                })
                .doOnError(throwable -> {
                    LOGGER.warn("Cannont connect!");
                });
    }

    private Ack toAck(DatagramPacket datagramPacket) {
        LOGGER.debug("[Node {}] toAck Datagram receipient{}", nodeId, datagramPacket.recipient());
        LOGGER.debug("[Node {}] toAck Datagram sender {}", nodeId, datagramPacket.sender());
        try {
            return Ack.parseFrom(NettyUtils.toByteArray(datagramPacket.content().retain()));
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

}
