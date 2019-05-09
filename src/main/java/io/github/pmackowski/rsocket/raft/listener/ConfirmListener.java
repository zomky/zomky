package io.github.pmackowski.rsocket.raft.listener;

public interface ConfirmListener {

    void handle(long index);

}
