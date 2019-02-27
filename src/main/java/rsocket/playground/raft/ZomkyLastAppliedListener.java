package rsocket.playground.raft;

import java.nio.ByteBuffer;

public interface ZomkyLastAppliedListener {

    void handle(long index, ByteBuffer response);

}
