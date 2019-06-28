package io.github.pmackowski.rsocket.raft;

import io.rsocket.Closeable;

public interface RaftServer extends Closeable {

    int getCurrentLeaderId();

    boolean isLeader();

    boolean isCandidate();

    boolean isFollower();

    void addServer(int newServer);
}
