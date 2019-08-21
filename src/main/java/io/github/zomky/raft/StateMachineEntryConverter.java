package io.github.zomky.raft;

import io.rsocket.Payload;

public interface StateMachineEntryConverter {

    byte[] convert(Payload payload);

}
