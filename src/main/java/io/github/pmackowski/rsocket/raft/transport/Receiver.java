package io.github.pmackowski.rsocket.raft.transport;

import com.google.protobuf.AbstractMessageLite;
import com.google.protobuf.InvalidProtocolBufferException;
import io.github.pmackowski.rsocket.raft.InnerNode;
import io.github.pmackowski.rsocket.raft.client.protobuf.InfoRequest;
import io.github.pmackowski.rsocket.raft.gossip.GossipProtocol;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.InitJoinRequest;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.InitLeaveRequest;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.JoinRequest;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.LeaveRequest;
import io.github.pmackowski.rsocket.raft.raft.RaftException;
import io.github.pmackowski.rsocket.raft.raft.RaftProtocol;
import io.github.pmackowski.rsocket.raft.transport.protobuf.*;
import io.github.pmackowski.rsocket.raft.utils.NettyUtils;
import io.rsocket.*;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.ByteBufPayload;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.udp.UdpServer;

import java.time.Duration;

public class Receiver {

    private static final Logger LOGGER = LoggerFactory.getLogger(Receiver.class);

    private InnerNode node;

    private CloseableChannel raftReceiver, clientReceiver;

    private Connection gossipReceiver;

    public Receiver(InnerNode node) {
        this.node = node;
    }

    public void start() {
        gossipReceiver = UdpServer.create()
                .port(node.getNodeId()+20000)
                .handle(node.getGossipProtocol()::onPing)
                .bindNow(Duration.ofSeconds(1));

        raftReceiver = RSocketFactory.receive()
                .acceptor(new RaftSocketAcceptor(node))
                .transport(TcpServerTransport.create(node.getNodeId()))
                .start()
                .block();

        raftReceiver.onClose()
                .doFinally(signalType -> LOGGER.warn("[Node {}] Raft onClose", node.getNodeId()))
                .subscribe();

        clientReceiver = RSocketFactory.receive()
                .acceptor(new ClientSocketAcceptor(node))
                .transport(TcpServerTransport.create(node.getNodeId() + 10000))
                .start()
                .block();

        clientReceiver.onClose()
                .doFinally(signalType -> LOGGER.warn("[Node {}] Client onClose", node.getNodeId()))
                .subscribe();
    }

    public void stop() {
        gossipReceiver.dispose();
        raftReceiver.dispose();
        clientReceiver.dispose();
    }

    private static class ClientSocketAcceptor implements SocketAcceptor {

        private InnerNode node;

        public ClientSocketAcceptor(InnerNode node) {
            this.node = node;
        }

        @Override
        public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
            return Mono.just(new AbstractRSocket() {

                @Override
                public Mono<Payload> requestResponse(Payload payload) {
                    RaftProtocol raftProtocol = node.getRaftProtocol();
                    return raftProtocol.onClientRequest(payload);
                }

                @Override
                public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                    RaftProtocol raftProtocol = node.getRaftProtocol();
                    return raftProtocol.onClientRequests(payloads);
                }

            });
        }
    }

    private static class RaftSocketAcceptor implements SocketAcceptor {

        private InnerNode node;

        public RaftSocketAcceptor(InnerNode node) {
            this.node = node;
        }

        @Override
        public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
            return Mono.just(new AbstractRSocket() {
                @Override
                public Mono<Payload> requestResponse(Payload payload) {
                    MetadataRequest metadataRequest = toMetadataRequest(payload);
                    RpcType rpcType = RpcType.fromCode(metadataRequest.getMessageType());
                    String groupName = metadataRequest.getGroupName();
                    RaftProtocol raftProtocol = node.getRaftProtocol();
                    GossipProtocol gossipProtocol = node.getGossipProtocol();
                    switch (rpcType) {
                        case APPEND_ENTRIES:
                            return Mono.just(payload)
                                    .map(this::toAppendEntriesRequest)
                                    .flatMap(appendEntriesRequest -> raftProtocol.onAppendEntries(groupName, appendEntriesRequest))
                                    .map(this::toPayload);

                        case PRE_REQUEST_VOTE:
                            return Mono.just(payload)
                                    .map(this::toPreVoteRequest)
                                    .flatMap(preVoteRequest -> raftProtocol.onPreRequestVote(groupName, preVoteRequest))
                                    .map(this::toPayload);

                        case REQUEST_VOTE:
                            return Mono.just(payload)
                                    .map(this::toVoteRequest)
                                    .flatMap(voteRequest -> raftProtocol.onRequestVote(groupName, voteRequest))
                                    .map(this::toPayload);

                        case ADD_SERVER:
                            return Mono.just(payload)
                                    .map(this::toAddServerRequest)
                                    .flatMap(addServerRequest -> raftProtocol.onAddServer(groupName, addServerRequest))
                                    .map(this::toPayload);

                        case REMOVE_SERVER:
                            return Mono.just(payload)
                                    .map(this::toRemoveServerRequest)
                                    .flatMap(removeServerRequest -> raftProtocol.onRemoveServer(groupName, removeServerRequest))
                                    .map(this::toPayload);

                        case ADD_GROUP:
                            return Mono.just(payload)
                                    .map(this::toCreateGroupRequest)
                                    .flatMap(createGroupRequest -> raftProtocol.onAddGroup(groupName, createGroupRequest))
                                    .map(this::toPayload);

                        case INFO:
                            return Mono.just(payload)
                                    .map(this::toInfoRequest)
                                    .flatMap(infoRequest -> node.onInfoRequest(infoRequest))
                                    .map(this::toPayload);

                        case INIT_JOIN:
                            return Mono.just(payload)
                                    .map(this::toInitJoinRequest)
                                    .flatMap(gossipProtocol::join)
                                    .map(this::toPayload);

                        case JOIN:
                            return Mono.just(payload)
                                    .map(this::toJoinRequest)
                                    .flatMap(gossipProtocol::onJoinRequest)
                                    .map(this::toPayload);

                        case INIT_LEAVE:
                            return Mono.just(payload)
                                    .map(this::toInitLeaveRequest)
                                    .flatMap(gossipProtocol::onInitLeaveRequest)
                                    .map(this::toPayload);

                        case LEAVE:
                            return Mono.just(payload)
                                    .map(this::toLeaveRequest)
                                    .flatMap(gossipProtocol::onLeaveRequest)
                                    .map(this::toPayload);

                        default:
                            return Mono.error(new RaftException("??"));
                    }
                }

                private MetadataRequest toMetadataRequest(Payload payload) {
                    try {
                        return MetadataRequest.parseFrom(NettyUtils.toByteArray(payload.sliceMetadata()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid metadata!", e);
                    }
                }

                private AppendEntriesRequest toAppendEntriesRequest(Payload payload) {
                    try {
                        return AppendEntriesRequest.parseFrom(NettyUtils.toByteArray(payload.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid pre-vote request!", e);
                    }
                }

                private PreVoteRequest toPreVoteRequest(Payload payload) {
                    try {
                        return PreVoteRequest.parseFrom(NettyUtils.toByteArray(payload.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid pre-vote request!", e);
                    }
                }

                private VoteRequest toVoteRequest(Payload payload) {
                    try {
                        return VoteRequest.parseFrom(NettyUtils.toByteArray(payload.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid vote request!", e);
                    }
                }

                private AddServerRequest toAddServerRequest(Payload payload) {
                    try {
                        return AddServerRequest.parseFrom(NettyUtils.toByteArray(payload.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid add server request!", e);
                    }
                }

                private RemoveServerRequest toRemoveServerRequest(Payload payload) {
                    try {
                        return RemoveServerRequest.parseFrom(NettyUtils.toByteArray(payload.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid remove server request!", e);
                    }
                }

                private AddGroupRequest toCreateGroupRequest(Payload payload) {
                    try {
                        return AddGroupRequest.parseFrom(NettyUtils.toByteArray(payload.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid create group request!", e);
                    }
                }

                private InfoRequest toInfoRequest(Payload payload) {
                    try {
                        return InfoRequest.parseFrom(NettyUtils.toByteArray(payload.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid info request!", e);
                    }
                }

                private InitJoinRequest toInitJoinRequest(Payload payload) {
                    try {
                        return InitJoinRequest.parseFrom(NettyUtils.toByteArray(payload.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid init join request!", e);
                    }
                }

                private JoinRequest toJoinRequest(Payload payload) {
                    try {
                        return JoinRequest.parseFrom(NettyUtils.toByteArray(payload.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid join request!", e);
                    }
                }

                private InitLeaveRequest toInitLeaveRequest(Payload payload) {
                    try {
                        return InitLeaveRequest.parseFrom(NettyUtils.toByteArray(payload.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid init leave request!", e);
                    }
                }

                private LeaveRequest toLeaveRequest(Payload payload) {
                    try {
                        return LeaveRequest.parseFrom(NettyUtils.toByteArray(payload.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid leave request!", e);
                    }
                }

                private Payload toPayload(AbstractMessageLite abstractMessageLite) {
                    return ByteBufPayload.create(abstractMessageLite.toByteArray());
                }

            });
        }
    }

}
