package rsocket.playground.raft;

import java.io.Serializable;

public class RequestVoteResult implements Serializable {

    private long term;
    private boolean voteGranted;

    /**
     * currentTerm, for candidate to update itself
     * @param term
     * @return
     */
    public RequestVoteResult term(long term) {
        this.term = term;
        return this;
    }

    /**
     * true means candidate received vote
     * @param voteGranted
     * @return
     */
    public RequestVoteResult voteGranted(boolean voteGranted) {
        this.voteGranted = voteGranted;
        return this;
    }

    public long getTerm() {
        return term;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }
}
