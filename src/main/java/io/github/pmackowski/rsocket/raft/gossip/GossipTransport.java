package io.github.pmackowski.rsocket.raft.gossip;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ack;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ping;
import io.github.pmackowski.rsocket.raft.utils.NettyUtils;
import io.netty.channel.socket.DatagramPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.resources.LoopResources;
import reactor.netty.udp.UdpClient;

class GossipTransport {

    private static final Logger LOGGER = LoggerFactory.getLogger(GossipTransport.class);

    public Mono<Ack> ping(Ping ping) {
        return client(ping)
                .flatMap(connection ->
                        connection.outbound().sendByteArray(Mono.just(ping.toByteArray())).then()
                            .then(connection.inbound().receiveObject().cast(DatagramPacket.class).next()
                                    .doOnNext(i -> log(ping))
                                    .map(this::toAck)
                                    .onErrorResume(throwable -> {
                                        logError(ping, throwable);
                                        return Mono.empty();
                                    })
                            )
                );
    }

    private void log(Ping ping) {
        if (ping.getDirect()) {
            if (ping.getInitiatorNodeId() != ping.getRequestorNodeId()) {
                LOGGER.info("[Node {}][onPing] Direct probe to {} on behalf of {} successful.", ping.getRequestorNodeId(), ping.getDestinationNodeId(), ping.getInitiatorNodeId());
            } else {
                LOGGER.info("[Node {}][ping] Direct probe to {} successful.", ping.getInitiatorNodeId(), ping.getDestinationNodeId());
            }
        } else {
            LOGGER.info("[Node {}][ping] Indirect probe to {} through {} successful.", ping.getInitiatorNodeId(), ping.getDestinationNodeId(), ping.getRequestorNodeId());
        }
    }

    private void logError(Ping ping, Throwable throwable) {
        if (ping.getDirect()) {
            if (ping.getInitiatorNodeId() != ping.getRequestorNodeId()) {
                LOGGER.warn("[Node {}][onPing] Direct probe to {} on behalf of {} failed. Reason {}.", ping.getRequestorNodeId(), ping.getDestinationNodeId(), ping.getInitiatorNodeId(), throwable.getMessage());
            } else {
                LOGGER.warn("[Node {}][ping] Direct probe to {} failed. Reason {}.", ping.getInitiatorNodeId(), ping.getDestinationNodeId(), throwable.getMessage());
            }
        } else {
            LOGGER.warn("[Node {}][ping] Indirect probe to {} through {} failed. Reason {}", ping.getInitiatorNodeId(), ping.getDestinationNodeId(), ping.getRequestorNodeId(), throwable.getMessage());
        }
    }

    private Mono<? extends Connection> client(Ping ping) {
        return UdpClient.create()
                .port(ping.getDirect() ? ping.getDestinationNodeId() + 20000 : ping.getRequestorNodeId() + 20000)
                .runOn(LoopResources.create("gossip-" + ping.getRequestorNodeId()))
                .connect()
                .doOnCancel(() -> {
                    LOGGER.info("[Node {}] Probe to {} has been cancelled", ping.getInitiatorNodeId(), ping.getDestinationNodeId());
                })
                .doOnError(throwable -> {
                    LOGGER.warn("Cannot connect!");
                });
    }

    private Ack toAck(DatagramPacket datagramPacket) {
        try {
            return Ack.parseFrom(NettyUtils.toByteArray(datagramPacket.content().retain()));
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

}
