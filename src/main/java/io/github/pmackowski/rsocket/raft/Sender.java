package io.github.pmackowski.rsocket.raft;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.pmackowski.rsocket.raft.rpc.*;
import io.github.pmackowski.rsocket.raft.utils.NettyUtils;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.ByteBufPayload;
import reactor.core.publisher.Mono;

public class Sender {

    private final int nodeId;
    private final RSocket requestVoteSocket;
    private final RSocket appendEntriesSocket;
    private final boolean available;

    public static Sender availableSender(int nodeId, RSocket requestVoteSocket, RSocket appendEntriesSocket) {
        return new Sender(nodeId, requestVoteSocket, appendEntriesSocket, true);
    }

    public static Sender unavailableSender(int nodeId) {
        return new Sender(nodeId, null, null,false);
    }

    public Sender(int nodeId, RSocket requestVoteSocket, RSocket appendEntriesSocket, boolean available) {
        this.nodeId = nodeId;
        this.requestVoteSocket = requestVoteSocket;
        this.appendEntriesSocket = appendEntriesSocket;
        this.available = available;
    }

    public Mono<PreVoteResponse> requestPreVote(PreVoteRequest preVoteRequest) {
        Payload payload = ByteBufPayload.create(preVoteRequest.toByteArray(), "pre-vote".getBytes());
        return requestVoteSocket.requestResponse(payload)
                .map(payload1 -> {
                    try {
                        return PreVoteResponse.parseFrom(NettyUtils.toByteArray(payload1.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid pre-vote response!", e);
                    }
                });
    }

    public Mono<VoteResponse> requestVote(VoteRequest voteRequest) {
        Payload payload = ByteBufPayload.create(voteRequest.toByteArray());
        return requestVoteSocket.requestResponse(payload)
                .map(payload1 -> {
                    try {
                        return VoteResponse.parseFrom(NettyUtils.toByteArray(payload1.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid vote response!", e);
                    }
                });
    }

    public Mono<AppendEntriesResponse> appendEntries(AppendEntriesRequest appendEntriesRequest) {
        Payload payload = ByteBufPayload.create(appendEntriesRequest.toByteArray());
        return appendEntriesSocket.requestResponse(payload)
                .map(payload1 -> {
                    try {
                        return AppendEntriesResponse.parseFrom(NettyUtils.toByteArray(payload.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid append entries response!", e);
                    }
                });

    }

    public RSocket getAppendEntriesSocket() {
        return appendEntriesSocket;
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
        if (requestVoteSocket != null) requestVoteSocket.dispose();
        if (appendEntriesSocket != null) appendEntriesSocket.dispose();
    }
}
