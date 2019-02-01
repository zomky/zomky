package rsocket.playground.raft;

public interface NodeRepository {

    /**
     * Increases current term and set voteFor = nodeId
     * @param nodeId
     */
    void voteForMyself(int nodeId);

    void voteForCandidate(int nodeId, int candidateId, long term);

}
