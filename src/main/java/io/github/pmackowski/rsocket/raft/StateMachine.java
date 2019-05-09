package io.github.pmackowski.rsocket.raft;

import java.nio.ByteBuffer;

public interface StateMachine {

    ByteBuffer applyLogEntry(ByteBuffer entry);

}
