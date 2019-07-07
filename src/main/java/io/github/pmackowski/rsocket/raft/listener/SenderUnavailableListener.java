package io.github.pmackowski.rsocket.raft.listener;

import io.github.pmackowski.rsocket.raft.transport.Sender;

public interface SenderUnavailableListener {

    void handle(Sender sender);

}
