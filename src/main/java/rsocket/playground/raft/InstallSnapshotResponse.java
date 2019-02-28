package rsocket.playground.raft;

public class InstallSnapshotResponse {

    /**
     * currentTerm, for leader to update itself
     */
    private int term;

    public InstallSnapshotResponse term(int term) {
        this.term = term;
        return this;
    }
}
