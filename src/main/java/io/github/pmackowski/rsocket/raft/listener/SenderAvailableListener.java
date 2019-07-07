package io.github.pmackowski.rsocket.raft.listener;

import io.github.pmackowski.rsocket.raft.transport.Sender;

public interface SenderAvailableListener {

    void handle(Sender sender);

}
