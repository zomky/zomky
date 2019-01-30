package rsocket.playground.raft;

public class VoteRequest implements TermAware {

    private long term;
    private int candidateId;

    /**
     * candidate’s term
     * @param term
     * @return
     */
    public VoteRequest term(long term) {
        this.term = term;
        return this;
    }

    /**
     * candidate requesting vote
     * @param candidateId
     * @return
     */
    public VoteRequest candidateId(int candidateId) {
        this.candidateId = candidateId;
        return this;

    }

    @Override
    public long getTerm() {
        return term;
    }

    public int getCandidateId() {
        return candidateId;
    }

    @Override
    public String toString() {
        return "VoteRequest{" +
                "term=" + term +
                ", candidateId=" + candidateId +
                '}';
    }
}
