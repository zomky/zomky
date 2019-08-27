package io.github.zomky.gossip;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.zomky.gossip.protobuf.Ack;
import io.github.zomky.gossip.protobuf.Ping;
import io.github.zomky.raft.RaftException;
import io.github.zomky.transport.RpcType;
import io.github.zomky.transport.protobuf.MetadataRequest;
import io.github.zomky.utils.NettyUtils;
import io.netty.channel.socket.DatagramPacket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.ByteBufPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.resources.LoopResources;
import reactor.netty.udp.UdpClient;

class GossipTransport {

    private static final Logger LOGGER = LoggerFactory.getLogger(GossipTransport.class);

    public Flux<Ack> ping(Ping ping) {
        return client(ping)
                .flatMapMany(connection ->
                    connection.outbound()
                            .sendByteArray(Mono.just(ping.toByteArray()))
                            .then()
                            .thenMany(connection
                                    .inbound()
                                    .receiveObject()
                                    .cast(DatagramPacket.class)
                                    .map(this::toAck)
                            )
                );
    }

    public Mono<Ack> pingTcp(Ping ping) {
        Payload payload = ByteBufPayload.create(ping.toByteArray(), metadataRequest(RpcType.PING));
        return requestResponse(ping.getDestinationNodeId(), payload) // TODO destination address
                .map(payload1 -> {
                    try {
                        return Ack.parseFrom(NettyUtils.toByteArray(payload1.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid ack response!", e);
                    }
                });
    }

    private Mono<? extends Connection> client(Ping ping) {
        return UdpClient.create()
                .port(ping.getDirect() ? ping.getDestinationNodeId() + 20000 : ping.getRequestorNodeId() + 20000)
                .runOn(LoopResources.create("gossip-" + ping.getRequestorNodeId()))
                .connect()
                .doOnCancel(() -> {
                    LOGGER.debug("[Node {}] Probe to {} has been cancelled", ping.getInitiatorNodeId(), ping.getDestinationNodeId());
                })
                .doOnError(throwable -> {
                    LOGGER.warn("Cannot connect!");
                });
    }

    private Ack toAck(DatagramPacket datagramPacket) {
        try {
            return Ack.parseFrom(NettyUtils.toByteArray(datagramPacket.content().retain()));
        } catch (InvalidProtocolBufferException e) {
            throw new GossipException("datagram packet cannot be converted to Ack", e);
        }
    }

    // temporary here TODO
    private byte[] metadataRequest(RpcType rpcType) {
        MetadataRequest metadataRequest = MetadataRequest.newBuilder()
                .setMessageType(rpcType.getCode())
                .build();
        return metadataRequest.toByteArray();
    }

    private Mono<Payload> requestResponse(int nodeId, Payload payload) {
        return rSocketMono(nodeId).flatMap(rSocket -> rSocket.requestResponse(payload));
    }

    private Mono<RSocket> rSocketMono(int nodeId) {
        return RSocketFactory.connect()
                .transport(TcpClientTransport.create(nodeId))
                .start(); // TODO cache or object pool
    }
}
