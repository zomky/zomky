package io.github.pmackowski.rsocket.raft.transport;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.pmackowski.rsocket.raft.RaftException;
import io.github.pmackowski.rsocket.raft.transport.protobuf.*;
import io.github.pmackowski.rsocket.raft.utils.NettyUtils;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.ByteBufPayload;
import reactor.core.publisher.Mono;

public class Sender {

    private final int nodeId;
    private final RSocket raftSocket;
    private final boolean available;

    public static Sender createSender(int nodeId) {
        RSocket raftSocket = RSocketFactory.connect()
                .transport(TcpClientTransport.create(nodeId))
                .start()
                .block();

        return new Sender(nodeId, raftSocket, true);
    }

    public static Sender availableSender(int nodeId, RSocket raftSocket) {
        return new Sender(nodeId, raftSocket, true);
    }

    public static Sender unavailableSender(int nodeId) {
        return new Sender(nodeId, null,false);
    }

    public Sender(int nodeId, RSocket raftSocket, boolean available) {
        this.nodeId = nodeId;
        this.raftSocket = raftSocket;
        this.available = available;
    }

    public Mono<PreVoteResponse> requestPreVote(PreVoteRequest preVoteRequest) {
        Payload payload = ByteBufPayload.create(preVoteRequest.toByteArray(), new byte[] {RpcType.PRE_REQUEST_VOTE.getCode()});
        return raftSocket.requestResponse(payload)
                .map(payload1 -> {
                    try {
                        return PreVoteResponse.parseFrom(NettyUtils.toByteArray(payload1.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid pre-vote response!", e);
                    }
                });
    }

    public Mono<VoteResponse> requestVote(VoteRequest voteRequest) {
        Payload payload = ByteBufPayload.create(voteRequest.toByteArray(), new byte[] {RpcType.REQUEST_VOTE.getCode()});
        return raftSocket.requestResponse(payload)
                .map(payload1 -> {
                    try {
                        return VoteResponse.parseFrom(NettyUtils.toByteArray(payload1.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid vote response!", e);
                    }
                });
    }

    public Mono<AppendEntriesResponse> appendEntries(AppendEntriesRequest appendEntriesRequest) {
        Payload payload = ByteBufPayload.create(appendEntriesRequest.toByteArray(), new byte[] {RpcType.APPEND_ENTRIES.getCode()});
        return raftSocket.requestResponse(payload)
                .map(appendEntriesResponsePayload -> {
                    try {
                        return AppendEntriesResponse.parseFrom(NettyUtils.toByteArray(appendEntriesResponsePayload.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid append entries response!", e);
                    } finally {
                        appendEntriesResponsePayload.release();
                    }
                });
    }

    public Mono<AddServerResponse> addServer(AddServerRequest addServerRequest) {
        Payload payload = ByteBufPayload.create(addServerRequest.toByteArray(), new byte[] {RpcType.ADD_SERVER.getCode()});
        return raftSocket.requestResponse(payload)
                .map(payload1 -> {
                    try {
                        return AddServerResponse.parseFrom(NettyUtils.toByteArray(payload1.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid add server response!", e);
                    }
                });
    }

    public Mono<RemoveServerResponse> removeServer(RemoveServerRequest removeServerRequest) {
        Payload payload = ByteBufPayload.create(removeServerRequest.toByteArray(), new byte[] {RpcType.REMOVE_SERVER.getCode()});
        return raftSocket.requestResponse(payload)
                .map(payload1 -> {
                    try {
                        return RemoveServerResponse.parseFrom(NettyUtils.toByteArray(payload1.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid remove server response!", e);
                    }
                });
    }

    public int getNodeId() {
        return nodeId;
    }

    public boolean isAvailable() {
        return available;
    }

    public boolean isNotAvailable() {
        return !available;
    }

    @Override
    public String toString() {
        return "Sender{" +
                "nodeId=" + nodeId +
                ", available=" + available +
                '}';
    }

    public void stop() {
        if (raftSocket != null) raftSocket.dispose();
    }
}
