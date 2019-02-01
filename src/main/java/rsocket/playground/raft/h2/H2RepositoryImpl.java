package rsocket.playground.raft.h2;

import rsocket.playground.raft.NodeRepository;

public class H2RepositoryImpl implements NodeRepository {

    @Override
    public void voteForMyself(int nodeId) {
        H2.voteForItself(nodeId);
    }

    @Override
    public void voteForCandidate(int nodeId, int candidateId, long term) {
        H2.updateVotedFor(nodeId, candidateId, term);
    }
}
