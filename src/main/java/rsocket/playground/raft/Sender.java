package rsocket.playground.raft;

import io.rsocket.RSocket;
import reactor.util.annotation.Nullable;

import java.util.Optional;

public class Sender {

    private final int nodeId;
    private final RSocket rSocket;
    private final boolean available;

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

    @Override
    public String toString() {
        return "Sender{" +
                "nodeId=" + nodeId +
                ", available=" + available +
                '}';
    }
}
