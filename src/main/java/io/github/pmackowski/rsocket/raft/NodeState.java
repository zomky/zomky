package io.github.pmackowski.rsocket.raft;

public enum NodeState {

    PASSIVE,

    FOLLOWER,

    CANDIDATE,

    LEADER

}
