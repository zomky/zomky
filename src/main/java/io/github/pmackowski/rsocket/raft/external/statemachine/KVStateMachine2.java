package io.github.pmackowski.rsocket.raft.external.statemachine;

import io.github.pmackowski.rsocket.raft.annotation.ZomkyStateMachine;

@ZomkyStateMachine(name = "kv2")
public class KVStateMachine2 extends KVStateMachine {

    public KVStateMachine2(Integer nodeId) {
        super(2, nodeId);
    }
}
