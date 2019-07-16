package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.transport.protobuf.AddServerRequest;
import io.github.pmackowski.rsocket.raft.transport.protobuf.AddServerResponse;
import io.github.pmackowski.rsocket.raft.transport.protobuf.RemoveServerRequest;
import io.github.pmackowski.rsocket.raft.transport.protobuf.RemoveServerResponse;
import io.rsocket.Closeable;
import reactor.core.publisher.Mono;

import java.util.List;

public interface RaftServer extends Closeable {

    ElectionTimeout getElectionTimeout();

    Mono<AddServerResponse> onAddServer(String groupName, AddServerRequest addServerRequest);

    Mono<RemoveServerResponse> onRemoveServer(String groupName, RemoveServerRequest removeServerRequest);

    void addGroup(RaftGroup raftGroup);

    List<Integer> nodes();
}