package io.github.pmackowski.rsocket.raft;

import io.rsocket.Payload;

public interface StateMachineEntryConverter {

    byte[] convert(Payload payload);

}
