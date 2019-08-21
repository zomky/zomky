package io.github.zomky.external.statemachine;

import io.github.zomky.annotation.ZomkyStateMachine;

@ZomkyStateMachine(name = "kv1")
public class KVStateMachine1 extends KVStateMachine {

    public KVStateMachine1(Integer nodeId) {
        super(1, nodeId);
    }
}
