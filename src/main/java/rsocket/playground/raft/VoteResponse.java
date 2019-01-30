package rsocket.playground.raft;

public class VoteResponse implements TermAware {

    static final VoteResponse FALLBACK_RESPONSE = new VoteResponse().voteGranted(false);

    private long term;
    private boolean voteGranted;

    /**
     * currentTerm, for candidate to update itself
     * @param term
     * @return
     */
    public VoteResponse term(long term) {
        this.term = term;
        return this;
    }

    /**
     * true means candidate received vote
     * @param voteGranted
     * @return
     */
    public VoteResponse voteGranted(boolean voteGranted) {
        this.voteGranted = voteGranted;
        return this;
    }

    @Override
    public long getTerm() {
        return term;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }
}
