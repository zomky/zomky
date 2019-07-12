package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.transport.protobuf.AddServerRequest;
import io.github.pmackowski.rsocket.raft.transport.protobuf.AddServerResponse;
import io.github.pmackowski.rsocket.raft.transport.protobuf.RemoveServerRequest;
import io.github.pmackowski.rsocket.raft.transport.protobuf.RemoveServerResponse;
import io.rsocket.Closeable;
import reactor.core.publisher.Mono;

public interface RaftServer extends Closeable {

    ElectionTimeout getElectionTimeout();

    int getCurrentLeaderId();

    boolean isLeader();

    boolean isCandidate();

    boolean isFollower();

    boolean isPassive();

    Mono<AddServerResponse> onAddServer(AddServerRequest addServerRequest);

    Mono<RemoveServerResponse> onRemoveServer(RemoveServerRequest removeServerRequest);
}