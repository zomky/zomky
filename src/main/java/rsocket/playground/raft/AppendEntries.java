package rsocket.playground.raft;

import java.io.Serializable;

public class AppendEntries implements Serializable {

    private long term;
    private long leaderId;

    /**
     * set leader’s term
     * @param term
     * @return
     */
    public AppendEntries term(long term) {
        this.term = term;
        return this;
    }

    /**
     * so follower can redirect clients
     * @param leaderId
     * @return
     */
    public AppendEntries leaderId(long leaderId) {
        this.leaderId = leaderId;
        return this;
    }

    public long getTerm() {
        return term;
    }

    public long getLeaderId() {
        return leaderId;
    }

    @Override
    public String toString() {
        return "AppendEntries{" +
                "term=" + term +
                ", leaderId=" + leaderId +
                '}';
    }
}
