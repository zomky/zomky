package io.github.pmackowski.rsocket.raft.transport;

import com.google.protobuf.AbstractMessageLite;
import com.google.protobuf.InvalidProtocolBufferException;
import io.github.pmackowski.rsocket.raft.InternalRaftServer;
import io.github.pmackowski.rsocket.raft.RaftException;
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

public class Receiver {

    private static final Logger LOGGER = LoggerFactory.getLogger(Receiver.class);

    private InternalRaftServer node;
    private int nodeId;

    private CloseableChannel raftReceiver, clientReceiver;

    public Receiver(InternalRaftServer node, int nodeId) {
        this.node = node;
        this.nodeId = nodeId;
    }

    public void start() {
        raftReceiver = RSocketFactory.receive()
                .acceptor(new RaftSocketAcceptor(node))
                .transport(TcpServerTransport.create(nodeId))
                .start()
                .block();

        raftReceiver.onClose()
                .doFinally(signalType -> LOGGER.warn("[Node {}] Raft onClose", nodeId))
                .subscribe();

        clientReceiver = RSocketFactory.receive()
                .acceptor(new ClientSocketAcceptor(node))
                .transport(TcpServerTransport.create(nodeId + 10000))
                .start()
                .block();

        clientReceiver.onClose()
                .doFinally(signalType -> LOGGER.warn("[Node {}] Client onClose", nodeId))
                .subscribe();

    }

    public void stop() {
        raftReceiver.dispose();
        clientReceiver.dispose();
    }

    private static class ClientSocketAcceptor implements SocketAcceptor {

        private InternalRaftServer node;

        public ClientSocketAcceptor(InternalRaftServer node) {
            this.node = node;
        }

        @Override
        public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
            return Mono.just(new AbstractRSocket() {

                @Override
                public Mono<Payload> requestResponse(Payload payload) {
                    return node.onClientRequest(payload);
                }

                @Override
                public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                    return node.onClientRequests(payloads);
                }

            });
        }
    }

    private static class RaftSocketAcceptor implements SocketAcceptor {

        private InternalRaftServer node;

        public RaftSocketAcceptor(InternalRaftServer node) {
            this.node = node;
        }

        @Override
        public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
            return Mono.just(new AbstractRSocket() {
                @Override
                public Mono<Payload> requestResponse(Payload payload) {
                    RpcType rpcType = RpcType.fromPayload(payload);
                    switch (rpcType) {
                        case APPEND_ENTRIES:
                            return Mono.just(payload)
                                .map(this::toAppendEntriesRequest)
                                .flatMap(appendEntriesRequest -> node.onAppendEntries(appendEntriesRequest))
                                .map(this::toPayload);

                        case PRE_REQUEST_VOTE:
                            return Mono.just(payload)
                                    .map(this::toPreVoteRequest)
                                    .flatMap(preVoteRequest -> node.onPreRequestVote(preVoteRequest))
                                    .map(this::toPayload);

                        case REQUEST_VOTE:
                            return Mono.just(payload)
                                    .map(this::toVoteRequest)
                                    .flatMap(voteRequest -> node.onRequestVote(voteRequest))
                                    .map(this::toPayload);

                        case ADD_SERVER:
                            return Mono.just(payload)
                                    .map(this::toAddServerRequest)
                                    .flatMap(addServerRequest -> node.onAddServer(addServerRequest))
                                    .map(this::toPayload);

                        case REMOVE_SERVER:
                            return Mono.just(payload)
                                    .map(this::toRemoveServerRequest)
                                    .flatMap(removeServerRequest -> node.onRemoveServer(removeServerRequest))
                                    .map(this::toPayload);

                        default:
                            return Mono.error(new RaftException("??"));
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

                private Payload toPayload(AbstractMessageLite abstractMessageLite) {
                    return ByteBufPayload.create(abstractMessageLite.toByteArray());
                }

            });
        }
    }

}
