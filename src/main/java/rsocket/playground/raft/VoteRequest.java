package rsocket.playground.raft;

public class VoteRequest implements TermAware {

    private int term;
    private int candidateId;

    private long lastLogIndex;
    private long lastLogTerm;

    /**
     * candidate’s term
     * @param term
     * @return
     */
    public VoteRequest term(int term) {
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

    /**
     * index of candidate’s last log entry (§5.4)
     * @param lastLogIndex
     * @return
     */
    public VoteRequest lastLogIndex(long lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
        return this;
    }

    /**
     * term of candidate’s last log entry (§5.4)
     * @param lastLogTerm
     * @return
     */
    public VoteRequest lastLogTerm(long lastLogTerm) {
        this.lastLogTerm = lastLogTerm;
        return this;
    }

    @Override
    public int getTerm() {
        return term;
    }

    public int getCandidateId() {
        return candidateId;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public long getLastLogTerm() {
        return lastLogTerm;
    }

    @Override
    public String toString() {
        return "VoteRequest{" +
                "term=" + term +
                ", candidateId=" + candidateId +
                '}';
    }
}
