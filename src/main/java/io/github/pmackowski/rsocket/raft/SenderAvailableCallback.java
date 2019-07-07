package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.transport.Sender;

public interface SenderAvailableCallback {

    void handle(Sender sender);

}
