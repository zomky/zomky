package rsocket.playground.raft;

public class AppendEntriesResponse implements TermAware {

    private long term;
    private boolean success;

    /**
     * currentTerm, for leader to update itself
     * @param term
     * @return
     */
    public AppendEntriesResponse term(long term) {
        this.term = term;
        return this;
    }

    public AppendEntriesResponse success(boolean success) {
        this.success = success;
        return this;
    }

    @Override
    public long getTerm() {
        return term;
    }


    public boolean isSuccess() {
        return success;
    }

    @Override
    public String toString() {
        return "AppendEntriesResponse{" +
                "term=" + term +
                ", success=" + success +
                '}';
    }
}
