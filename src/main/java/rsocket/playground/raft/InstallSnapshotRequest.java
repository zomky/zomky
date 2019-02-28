package rsocket.playground.raft;

public class InstallSnapshotRequest {

    /**
     * leader’s term
     */
    private int term;

    /**
     * so follower can redirect clients
     */
    private int leaderId;

    /**
     * the snapshot replaces all entries up through and including this index
     */
    private long lastIncludedIndex;

    /**
     * lastIncludedTerm term of lastIncludedIndex
     * offset byte offset where chunk is positioned in the
     * snapshot file
     */
    private long lastIncludedTerm;


    /**
     * raw bytes of the snapshot chunk, starting at
     * offset
     */
    private byte[] data;

    /**
     *  true if this is the last chunk
     */
    private boolean done;

    public InstallSnapshotRequest term(int term) {
        this.term = term;
        return this;
    }

    public InstallSnapshotRequest leaderId(int leaderId) {
        this.leaderId = leaderId;
        return this;
    }

    public InstallSnapshotRequest lastIncludedIndex(long lastIncludedIndex) {
        this.lastIncludedIndex = lastIncludedIndex;
        return this;
    }

    public InstallSnapshotRequest lastIncludedTerm(long lastIncludedTerm) {
        this.lastIncludedTerm = lastIncludedTerm;
        return this;
    }

    public InstallSnapshotRequest data(byte[] data) {
        this.data = data;
        return this;
    }

    public InstallSnapshotRequest done(boolean done) {
        this.done = done;
        return this;
    }

    public int getTerm() {
        return term;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public long getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    public long getLastIncludedTerm() {
        return lastIncludedTerm;
    }

    public byte[] getData() {
        return data;
    }

    public boolean isDone() {
        return done;
    }
}
