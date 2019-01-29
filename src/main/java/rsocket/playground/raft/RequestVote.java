package rsocket.playground.raft;

import java.io.Serializable;

public class RequestVote implements Serializable {

    private long term;
    private int candidateId;

    /**
     * candidate’s term
     * @param term
     * @return
     */
    public RequestVote term(long term) {
        this.term = term;
        return this;
    }

    /**
     * candidate requesting vote
     * @param candidateId
     * @return
     */
    public RequestVote candidateId(int candidateId) {
        this.candidateId = candidateId;
        return this;

    }

    public long getTerm() {
        return term;
    }

    public int getCandidateId() {
        return candidateId;
    }

    @Override
    public String toString() {
        return "RequestVote{" +
                "term=" + term +
                ", candidateId=" + candidateId +
                '}';
    }
}
