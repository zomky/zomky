package io.github.zomky.external.statemachine;

import io.github.zomky.annotation.ZomkyStateMachine;

@ZomkyStateMachine(name = "kv2")
public class KVStateMachine2 extends KVStateMachine {

    public KVStateMachine2(Integer nodeId) {
        super(2, nodeId);
    }
}
