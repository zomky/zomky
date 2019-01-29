package rsocket.playground.raft;

import java.io.Serializable;

public class AppendEntriesResult implements Serializable {

    private long term;

    /**
     * currentTerm, for leader to update itself
     * @param term
     * @return
     */
    public AppendEntriesResult term(long term) {
        this.term = term;
        return this;
    }

    public long getTerm() {
        return term;
    }

}
