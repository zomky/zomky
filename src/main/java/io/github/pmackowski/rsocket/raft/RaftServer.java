package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.rpc.AddServerRequest;
import io.github.pmackowski.rsocket.raft.rpc.AddServerResponse;
import io.rsocket.Closeable;
import reactor.core.publisher.Mono;

public interface RaftServer extends Closeable {

    ElectionTimeout getElectionTimeout();

    int getCurrentLeaderId();

    boolean isLeader();

    boolean isCandidate();

    boolean isFollower();

    Mono<AddServerResponse> onAddServer(AddServerRequest addServerRequest);

    void addServer(AddServerRequest newServer);

    void removeServer(int oldMember);
}
