package io.github.zomky.raft;

import io.github.zomky.storage.RaftStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

class CommitIndexUpdater {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommitIndexUpdater.class);

    /**
     * Reinitialized after election,
     * index of highest log entry
     * known to be replicated on server
     * (initialized to 0, increases monotonically)
     */
    private Map<Integer, Long> matchIndexes = new ConcurrentHashMap<>();
    private final Object lock = new Object();

    CommitIndexUpdater() {}

    CommitIndexUpdater(Map<Integer, Long> matchIndexes) {
        this.matchIndexes = matchIndexes;
    }

    /**
     * If there exists an N such that N > commitIndex, a majority
     * of matchIndex[i] ≥ N, and log[N].term == currentTerm:
     * set commitIndex = N
     *
     * @param raftStorage
     * @param raftGroup
     * @param nodeId
     * @param matchIndex
     */
    void update(RaftStorage raftStorage, RaftGroup raftGroup, int nodeId, long matchIndex) {
        long start = System.currentTimeMillis();
        int noCommittedRequired = raftGroup.quorum() - 1;
        matchIndexes.put(nodeId, matchIndex);
        long currentCommitIndex = raftGroup.getCommitIndex();
        long candidateCommitIndex = currentCommitIndex + 1;
        if (matchIndex <= currentCommitIndex) {
            return;
        }

        Stream<Long> candidateMIndexes = matchIndexes.values().stream()
                .filter(i -> i >= candidateCommitIndex && i <= matchIndex)
                .sorted(Comparator.reverseOrder());

        Iterator<Long> iterator = candidateMIndexes.iterator();
        while (iterator.hasNext()) {
            long next = iterator.next();
            int count = (int) matchIndexes.values().stream().filter(m -> m >= next).count();
            if (count >= noCommittedRequired) {
                if (raftStorage.getTermByIndex(next) == raftStorage.getTerm()) {
                    currentCommitIndex = next;
                    break;
                } else {
                    log(start);
                    return;
                }
            }
        }
        log(start);

        synchronized (lock) {
            long commitIndex = raftGroup.getCommitIndex();
            if (currentCommitIndex > commitIndex) {
                raftGroup.setCommitIndex(currentCommitIndex);
            }
        }
    }

    private void log(long start) {
        final long end = System.currentTimeMillis();
        long milliseconds = (end - start);
        if (milliseconds > 20) {
            LOGGER.warn("Commit index updater has finished in {} milliseconds!", milliseconds);
        }
    }

}
