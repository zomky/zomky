package rsocket.playground.raft;

import java.nio.ByteBuffer;

public interface StateMachine {

    ByteBuffer applyLogEntry(ByteBuffer entry);

}
