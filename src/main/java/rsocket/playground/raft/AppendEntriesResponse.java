package rsocket.playground.raft;

public class AppendEntriesResponse implements TermAware {

    private long term;

    /**
     * currentTerm, for leader to update itself
     * @param term
     * @return
     */
    public AppendEntriesResponse term(long term) {
        this.term = term;
        return this;
    }

    @Override
    public long getTerm() {
        return term;
    }

}
