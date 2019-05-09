package io.github.pmackowski.rsocket.raft;

public interface SenderUnavailableCallback {

    void handle(Sender sender);

}
