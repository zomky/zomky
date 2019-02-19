package rsocket.playground.raft;

public class AppendEntriesRequest implements TermAware {

    private int term;
    private int leaderId;

    private long prevLogIndex;
    private long prevLogTerm;

    private byte[] entries;
    private long leaderCommit;

    /**
     * set leader’s term
     * @param term
     * @return
     */
    public AppendEntriesRequest term(int term) {
        this.term = term;
        return this;
    }

    /**
     * so follower can redirect clients
     * @param leaderId
     * @return
     */
    public AppendEntriesRequest leaderId(int leaderId) {
        this.leaderId = leaderId;
        return this;
    }

    /**
     * index of log entry immediately preceding
     * new ones
     * @param prevLogIndex
     * @return
     */
    public AppendEntriesRequest prevLogIndex(long prevLogIndex) {
        this.prevLogIndex = prevLogIndex;
        return this;
    }

    /**
     * term of prevLogIndex entry
     * @param prevLogTerm
     * @return
     */
    public AppendEntriesRequest prevLogTerm(long prevLogTerm) {
        this.prevLogTerm = prevLogTerm;
        return this;
    }

    /**
     * log entries to store (empty for heartbeat;
     * may send more than one for efficiency)
     * @param entries
     * @return
     */
    public AppendEntriesRequest entries(byte[] entries) {
        this.entries = entries;
        return this;
    }

    /**
     * leader’s commitIndex
     * @param leaderCommit
     * @return
     */
    public AppendEntriesRequest leaderCommit(long leaderCommit) {
        this.leaderCommit = leaderCommit;
        return this;
    }

    @Override
    public int getTerm() {
        return term;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public long getPrevLogIndex() {
        return prevLogIndex;
    }

    public long getPrevLogTerm() {
        return prevLogTerm;
    }

    public byte[] getEntries() {
        return entries;
    }

    public long getLeaderCommit() {
        return leaderCommit;
    }

}
