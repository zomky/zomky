package rsocket.playground.raft;

public class AppendEntriesRequest implements TermAware {

    private long term;
    private long leaderId;

    /**
     * set leader’s term
     * @param term
     * @return
     */
    public AppendEntriesRequest term(long term) {
        this.term = term;
        return this;
    }

    /**
     * so follower can redirect clients
     * @param leaderId
     * @return
     */
    public AppendEntriesRequest leaderId(long leaderId) {
        this.leaderId = leaderId;
        return this;
    }

    @Override
    public long getTerm() {
        return term;
    }

    public long getLeaderId() {
        return leaderId;
    }

    @Override
    public String toString() {
        return "AppendEntriesRequest{" +
                "term=" + term +
                ", leaderId=" + leaderId +
                '}';
    }
}
