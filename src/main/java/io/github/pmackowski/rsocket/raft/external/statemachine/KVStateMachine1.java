package io.github.pmackowski.rsocket.raft.external.statemachine;

import io.github.pmackowski.rsocket.raft.annotation.ZomkyStateMachine;

@ZomkyStateMachine(name = "kv1")
public class KVStateMachine1 extends KVStateMachine {

    public KVStateMachine1(Integer nodeId) {
        super(1, nodeId);
    }
}
