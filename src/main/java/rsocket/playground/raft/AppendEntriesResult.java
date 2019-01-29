package rsocket.playground.raft;

import java.io.Serializable;

public class AppendEntriesResult implements Serializable {

    private long term;
    private boolean success;

    /**
     * currentTerm, for leader to update itself
     * @param term
     * @return
     */
    public AppendEntriesResult term(long term) {
        this.term = term;
        return this;
    }

    /**
     * true if follower contained entry matching
     * prevLogIndex and prevLogTerm
     * @param success
     * @return
     */
    public AppendEntriesResult success(boolean success) {
        this.success = success;
        return this;
    }

    public long getTerm() {
        return term;
    }

    public boolean isSuccess() {
        return success;
    }
}
