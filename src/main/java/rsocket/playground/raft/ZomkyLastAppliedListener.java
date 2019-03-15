package rsocket.playground.raft;

import io.netty.buffer.ByteBuf;

public interface ZomkyLastAppliedListener {

    void handle(long index, ByteBuf response);

}
