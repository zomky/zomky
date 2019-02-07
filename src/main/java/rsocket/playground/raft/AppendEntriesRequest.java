package rsocket.playground.raft;

import java.util.HashSet;
import java.util.Set;

public class AppendEntriesRequest implements TermAware {

    private long term;
    private int leaderId;

    private long prevLogIndex;
    private long prevLogTerm;

    private Set<LogEntry> entries = new HashSet<>();
    private long leaderCommit;

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
    public AppendEntriesRequest entries(Set<LogEntry> entries) {
        this.entries = entries;
        return this;
    }

    public AppendEntriesRequest addEntry(LogEntry entry) {
        this.entries.add(entry);
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
    public long getTerm() {
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

    public Set<LogEntry> getEntries() {
        return entries;
    }

    public long getLeaderCommit() {
        return leaderCommit;
    }

    @Override
    public String toString() {
        return "AppendEntriesRequest{" +
                "term=" + term +
                ", leaderId=" + leaderId +
                '}';
    }
}
