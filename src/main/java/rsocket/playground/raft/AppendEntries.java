package rsocket.playground.raft;

import java.io.Serializable;

public class AppendEntries implements Serializable {

    private long term;
    private long leaderId;
    private long prevLogIndex;
    private long prevLogTerm;

    private long[] entries;
    private long leaderCommit;

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

    /**
     * index of log entry immediately preceding
     * new ones
     * @param prevLogIndex
     * @return
     */
    public AppendEntries prevLogIndex(long prevLogIndex) {
        this.prevLogIndex = prevLogIndex;
        return this;
    }

    /**
     * term of prevLogIndex entry
     * @param prevLogTerm
     * @return
     */
    public AppendEntries prevLogTerm(long prevLogTerm) {
        this.prevLogTerm = prevLogTerm;
        return this;
    }

    /**
     * log entries to store (empty for heartbeat;
     * may send more than one for efficiency)
     * @param entries
     * @return
     */
    public AppendEntries entries(long[] entries) {
        this.entries = entries;
        return this;
    }

    /**
     * leader’s commitIndex
     * @param leaderCommit
     * @return
     */
    public AppendEntries leaderCommit(long leaderCommit) {
        this.leaderCommit = leaderCommit;
        return this;
    }

    public long getTerm() {
        return term;
    }

    public long getLeaderId() {
        return leaderId;
    }

    public long getPrevLogIndex() {
        return prevLogIndex;
    }

    public long getPrevLogTerm() {
        return prevLogTerm;
    }

    public long[] getEntries() {
        return entries;
    }

    public long getLeaderCommit() {
        return leaderCommit;
    }
}
