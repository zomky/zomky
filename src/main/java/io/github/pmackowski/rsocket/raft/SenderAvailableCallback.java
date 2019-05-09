package io.github.pmackowski.rsocket.raft;

public interface SenderAvailableCallback {

    void handle(Sender sender);

}
