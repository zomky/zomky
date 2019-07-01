package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.storage.RaftStorage;

import java.util.Map;

class CommitIndexCalculator {

    /**
     * If there exists an N such that N > commitIndex, a majority
     * of matchIndex[i] ≥ N, and log[N].term == currentTerm:
     * set commitIndex = N
     *
     * @param raftStorage
     * @param node
     * @param matchIndex
     * @param lastIndex
     * @return
     */
    long calculate(RaftStorage raftStorage, DefaultRaftServer node, Map<Integer, Long> matchIndex, long lastIndex) {
        int noCommittedRequired = node.quorum() - 1;
        long n = lastIndex;

        while (n > node.getCommitIndex()) {
            long committed = committed(matchIndex, n);
            if (committed >= noCommittedRequired && raftStorage.getTermByIndex(n) == raftStorage.getTerm()) {
                break;
            } else {
                n--;
            }
        }
        return n;
    }

    private long committed(Map<Integer, Long> matchIndex, long idx) {
        return matchIndex.values().stream().filter(matchIndex1 -> matchIndex1 >= idx).count();
    }

}
