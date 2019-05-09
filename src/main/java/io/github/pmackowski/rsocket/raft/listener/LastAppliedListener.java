package io.github.pmackowski.rsocket.raft.listener;

import io.netty.buffer.ByteBuf;

public interface LastAppliedListener {

    void handle(long index, ByteBuf response);

}
