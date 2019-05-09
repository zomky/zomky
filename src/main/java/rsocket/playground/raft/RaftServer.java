package rsocket.playground.raft;

import io.rsocket.Closeable;

public interface RaftServer extends Closeable {

    int getCurrentLeaderId();

    boolean isLeader();

    boolean isFollower();

}
