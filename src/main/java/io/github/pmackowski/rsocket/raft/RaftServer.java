package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.rpc.AddServerRequest;
import io.rsocket.Closeable;

public interface RaftServer extends Closeable {

    ElectionTimeout getElectionTimeout();

    int getCurrentLeaderId();

    boolean isLeader();

    boolean isCandidate();

    boolean isFollower();

    void addServer(AddServerRequest newServer);

    void removeServer(int oldMember);
}
