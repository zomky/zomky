package io.github.zomky.listener;

import io.netty.buffer.ByteBuf;

public interface LastAppliedListener {

    void handle(long index, ByteBuf response);

}
