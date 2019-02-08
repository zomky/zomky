package rsocket.playground.raft;

import io.rsocket.RSocket;
import reactor.util.annotation.Nullable;

public class Sender {

    private final int nodeId;
    private final RSocket rSocket;
    private final boolean available;

    /**
     * Reinitialized after election,
     * index of the next log entry
     * to send to that server (initialized to leader
     * last log index + 1)
     */
    private volatile long nextIndex;

    /**
     * Reinitialized after election,
     * index of highest log entry
     * known to be replicated on server
     * (initialized to 0, increases monotonically)
     */
    private volatile long matchIndex;

    public static Sender availableSender(int nodeId, RSocket rSocket) {
        return new Sender(nodeId, rSocket, true);
    }

    public static Sender unavailableSender(int nodeId) {
        return new Sender(nodeId, null, false);
    }

    public Sender(int nodeId, RSocket rSocket, boolean available) {
        this.nodeId = nodeId;
        this.rSocket = rSocket;
        this.available = available;
    }

    @Nullable
    public RSocket getRSocket() {
        return rSocket;
    }

    public int getNodeId() {
        return nodeId;
    }

    public boolean isAvailable() {
        return available;
    }

    public boolean isNotAvailable() {
        return !available;
    }

    public long getNextIndex() {
        return nextIndex;
    }

    public long getMatchIndex() {
        return matchIndex;
    }

    @Override
    public String toString() {
        return "Sender{" +
                "nodeId=" + nodeId +
                ", available=" + available +
                '}';
    }
}
