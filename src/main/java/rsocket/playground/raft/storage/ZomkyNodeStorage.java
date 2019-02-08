package rsocket.playground.raft.storage;

import rsocket.playground.raft.LogEntry;
import rsocket.playground.raft.NodeData;

public interface ZomkyNodeStorage {

    /**
     * Increases current term and set voteFor = nodeId
     * @param nodeId
     */
    void voteForMyself(int nodeId);

    void voteForCandidate(int nodeId, int candidateId, long term);

    void updateTerm(int nodeId, long currentTerm);

    void appendLog(int nodeId, long index, long term, String content);

    LogEntry getLast(int nodeId);

    LogEntry getByIndex(int nodeId, long index);

    NodeData readNodeData(int nodeId);
}
