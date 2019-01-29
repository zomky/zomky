package rsocket.playground.raft;

import java.io.Serializable;

public class RequestVote implements Serializable {

    private long term;
    private int candidateId;
    private long lastLogIndex;
    private long lastLogTerm;

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

    /**
     * index of candidate’s last log entry (§5.4)
     * @param lastLogIndex
     * @return
     */
    public RequestVote lastLogIndex(long lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
        return this;

    }

    /**
     * term of candidate’s last log entry (§5.4)
     * @param lastLogTerm
     * @return
     */
    public RequestVote lastLogTerm(long lastLogTerm) {
        this.lastLogTerm = lastLogTerm;
        return this;
    }

    public long getTerm() {
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
        return "RequestVote{" +
                "term=" + term +
                ", candidateId=" + candidateId +
                ", lastLogIndex=" + lastLogIndex +
                ", lastLogTerm=" + lastLogTerm +
                '}';
    }
}
