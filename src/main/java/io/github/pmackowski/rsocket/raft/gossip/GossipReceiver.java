package io.github.pmackowski.rsocket.raft.gossip;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.pmackowski.rsocket.raft.InnerNode;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.GossipMetadataRequest;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.PingReqRequest;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.PingRequest;
import io.github.pmackowski.rsocket.raft.raft.RaftException;
import io.github.pmackowski.rsocket.raft.utils.NettyUtils;
import io.rsocket.*;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class GossipReceiver {

    private static final Logger LOGGER = LoggerFactory.getLogger(GossipReceiver.class);

    private InnerNode node;

    private CloseableChannel gossipReceiver;

    public GossipReceiver(InnerNode node) {
        this.node = node;
    }

    public void start() {
        gossipReceiver = RSocketFactory.receive()
                .acceptor(new GossipSocketAcceptor(node))
                .transport(TcpServerTransport.create(node.getNodeId() + 20000))
                .start()
                .block();

        gossipReceiver.onClose()
                .doFinally(signalType -> LOGGER.warn("[Node {}] Gossip onClose", node.getNodeId()))
                .subscribe();
    }

    private static class GossipSocketAcceptor implements SocketAcceptor {

        private InnerNode node;

        public GossipSocketAcceptor(InnerNode node) {
            this.node = node;
        }

        @Override
        public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
            return Mono.just(new AbstractRSocket() {

                @Override
                public Mono<Void> fireAndForget(Payload payload) {
                    GossipMetadataRequest metadataRequest = toMetadataRequest(payload);
                    GossipRpcType gossipRpcType = GossipRpcType.fromCode(metadataRequest.getMessageType());
                    switch (gossipRpcType) {
                        case PING:
                            return Mono.just(payload)
                                    .map(this::toPingRequest)
                                    .flatMap(ping -> node.onPingRequest(ping));

                        case PING_REQ:
                            return Mono.just(payload)
                                    .map(this::toPingReqRequest)
                                    .flatMap(pingReq -> node.onPingReqRequest(pingReq));
                        default:
                            throw new RuntimeException("?");
                    }
                }

                private GossipMetadataRequest toMetadataRequest(Payload payload) {
                    try {
                        return GossipMetadataRequest.parseFrom(NettyUtils.toByteArray(payload.sliceMetadata()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid gossip metadata!", e);
                    }
                }

                private PingRequest toPingRequest(Payload payload) {
                    try {
                        return PingRequest.parseFrom(NettyUtils.toByteArray(payload.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid ping request!", e);
                    }
                }

                private PingReqRequest toPingReqRequest(Payload payload) {
                    try {
                        return PingReqRequest.parseFrom(NettyUtils.toByteArray(payload.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid ping req request!", e);
                    }
                }

            });
        }
    }

}
