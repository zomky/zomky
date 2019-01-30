package rsocket.playground.raft;

import java.io.Serializable;

public interface TermAware extends Serializable {

    long getTerm();

}
