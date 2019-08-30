package io.github.zomky.raft;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.zomky.transport.RpcType;
import io.github.zomky.transport.protobuf.*;
import io.github.zomky.utils.NettyUtils;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.ByteBufPayload;
import reactor.core.publisher.Mono;

public class RaftTcpTransport {

    public static Mono<PreVoteResponse> requestPreVote(int nodeId, RaftGroup raftGroup, PreVoteRequest preVoteRequest) {
        Payload payload = ByteBufPayload.create(preVoteRequest.toByteArray(), metadataRequest(raftGroup.getGroupName(), RpcType.PRE_REQUEST_VOTE));
        return requestResponse(nodeId, payload)
                .map(payload1 -> {
                    try {
                        return PreVoteResponse.parseFrom(NettyUtils.toByteArray(payload1.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid pre-vote response!", e);
                    }
                });
    }

    public Mono<VoteResponse> requestVote(int nodeId, RaftGroup raftGroup, VoteRequest voteRequest) {
        Payload payload = ByteBufPayload.create(voteRequest.toByteArray(), metadataRequest(raftGroup.getGroupName(), RpcType.REQUEST_VOTE));
        return requestResponse(nodeId, payload)
                .map(payload1 -> {
                    try {
                        return VoteResponse.parseFrom(NettyUtils.toByteArray(payload1.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid vote response!", e);
                    }
                });
    }

    public Mono<AppendEntriesResponse> appendEntries(int nodeId, RaftGroup raftGroup, AppendEntriesRequest appendEntriesRequest) {
        Payload payload = ByteBufPayload.create(appendEntriesRequest.toByteArray(), metadataRequest(raftGroup.getGroupName(), RpcType.APPEND_ENTRIES));
        return requestResponse(nodeId, payload)
                .map(appendEntriesResponsePayload -> {
                    try {
                        return AppendEntriesResponse.parseFrom(NettyUtils.toByteArray(appendEntriesResponsePayload.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid append entries response!", e);
                    } finally {
                        //appendEntriesResponsePayload.release();
                    }
                });
    }

    public Mono<AddServerResponse> addServer(int nodeId, String groupName, AddServerRequest addServerRequest) {
        Payload payload = ByteBufPayload.create(addServerRequest.toByteArray(), metadataRequest(groupName, RpcType.ADD_SERVER));
        return requestResponse(nodeId, payload)
                .map(payload1 -> {
                    try {
                        return AddServerResponse.parseFrom(NettyUtils.toByteArray(payload1.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid add server response!", e);
                    }
                });
    }

    public Mono<RemoveServerResponse> removeServer(int nodeId, String groupName, RemoveServerRequest removeServerRequest) {
        Payload payload = ByteBufPayload.create(removeServerRequest.toByteArray(), metadataRequest(groupName, RpcType.REMOVE_SERVER));
        return requestResponse(nodeId, payload)
                .map(payload1 -> {
                    try {
                        return RemoveServerResponse.parseFrom(NettyUtils.toByteArray(payload1.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid remove server response!", e);
                    }
                });
    }

    public Mono<AddGroupResponse> addGroup(int nodeId, String groupName, AddGroupRequest createGroupRequest) {
        Payload payload = ByteBufPayload.create(createGroupRequest.toByteArray(), metadataRequest(groupName, RpcType.ADD_GROUP));
        return requestResponse(nodeId, payload)
                .map(payload1 -> {
                    try {
                        return AddGroupResponse.parseFrom(NettyUtils.toByteArray(payload1.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid add group response!", e);
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

    private static byte[] metadataRequest(String groupName, RpcType rpcType) {
        MetadataRequest metadataRequest = MetadataRequest.newBuilder()
                .setGroupName(groupName)
                .setMessageType(rpcType.getCode())
                .build();
        return metadataRequest.toByteArray();
    }

}
