package rsocket.playground.raft;

public class NodeData { // TODO should be persistent

    private long currentTerm;

    private Integer votedFor;

    /**
     * latest term server has seen (initialized to 0
     * on first boot, increases monotonically)
     *
     * @param currentTerm
     * @return
     */
    public NodeData currentTerm(long currentTerm) {
        this.currentTerm = currentTerm;
        return this;
    }

    /**
     * candidateId that received vote in current
     * term (or null if none)
     *
     * @param votedFor
     * @return
     */
    public NodeData votedFor(Integer votedFor) {
        this.votedFor = votedFor;
        return this;
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    public Integer getVotedFor() {
        return votedFor;
    }
}
