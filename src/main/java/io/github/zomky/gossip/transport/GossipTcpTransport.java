package io.github.zomky.gossip.transport;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.zomky.gossip.protobuf.*;
import io.github.zomky.raft.RaftException;
import io.github.zomky.transport.RpcType;
import io.github.zomky.transport.protobuf.MetadataRequest;
import io.github.zomky.utils.NettyUtils;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.ByteBufPayload;
import reactor.core.publisher.Mono;

public class GossipTcpTransport {

    public static Mono<InitJoinResponse> initJoin(InitJoinRequest initJoinRequest) {
        Payload payload = ByteBufPayload.create(initJoinRequest.toByteArray(), metadataRequest(RpcType.INIT_JOIN));
        return requestResponse(initJoinRequest.getRequesterPort(), payload)
                .map(payload1 -> {
                    try {
                        return InitJoinResponse.parseFrom(NettyUtils.toByteArray(payload1.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid init join response!", e);
                    }
                });
    }

    public static Mono<JoinResponse> join(JoinRequest joinRequest) {
        Payload payload = ByteBufPayload.create(joinRequest.toByteArray(), metadataRequest(RpcType.JOIN));
        return requestResponse(joinRequest.getPort(), payload)
                .map(payload1 -> {
                    try {
                        return JoinResponse.parseFrom(NettyUtils.toByteArray(payload1.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid join response!", e);
                    }
                });
    }

    public static Mono<InitLeaveResponse> initLeave(InitLeaveRequest initLeaveRequest) {
        Payload payload = ByteBufPayload.create(initLeaveRequest.toByteArray(), metadataRequest(RpcType.INIT_LEAVE));
        return requestResponse(initLeaveRequest.getRequesterPort(), payload)
                .map(payload1 -> {
                    try {
                        return InitLeaveResponse.parseFrom(NettyUtils.toByteArray(payload1.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid init leave response!", e);
                    }
                });
    }

    public static Mono<LeaveResponse> leave(LeaveRequest leaveRequest) {
        Payload payload = ByteBufPayload.create(leaveRequest.toByteArray(), metadataRequest(RpcType.LEAVE));
        return requestResponse(leaveRequest.getRequesterPort(), payload) // TODO port
                .map(payload1 -> {
                    try {
                        return LeaveResponse.parseFrom(NettyUtils.toByteArray(payload1.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid leave response!", e);
                    }
                });
    }

    private static Mono<RSocket> rSocketMono(int nodeId) {
        return RSocketFactory.connect()
                .transport(TcpClientTransport.create(nodeId))
                .start(); // TODO cache or object pool
    }

    private static Mono<Payload> requestResponse(int nodeId, Payload payload) {
        return rSocketMono(nodeId).flatMap(rSocket -> rSocket.requestResponse(payload));
    }

    private static byte[] metadataRequest(RpcType rpcType) {
        MetadataRequest metadataRequest = MetadataRequest.newBuilder()
                .setMessageType(rpcType.getCode())
                .build();
        return metadataRequest.toByteArray();
    }

}
