package io.github.pmackowski.rsocket.raft;

import io.rsocket.RSocket;

public class Sender {

    private final int nodeId;
    private final RSocket requestVoteSocket;
    private final RSocket appendEntriesSocket;
    private final boolean available;

    public static Sender availableSender(int nodeId, RSocket requestVoteSocket, RSocket appendEntriesSocket) {
        return new Sender(nodeId, requestVoteSocket, appendEntriesSocket, true);
    }

    public static Sender unavailableSender(int nodeId) {
        return new Sender(nodeId, null, null,false);
    }

    public Sender(int nodeId, RSocket requestVoteSocket, RSocket appendEntriesSocket, boolean available) {
        this.nodeId = nodeId;
        this.requestVoteSocket = requestVoteSocket;
        this.appendEntriesSocket = appendEntriesSocket;
        this.available = available;
    }

    public RSocket getRequestVoteSocket() {
        return requestVoteSocket;
    }

    public RSocket getAppendEntriesSocket() {
        return appendEntriesSocket;
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

    public void stop() {
        if (requestVoteSocket != null) requestVoteSocket.dispose();
        if (appendEntriesSocket != null) appendEntriesSocket.dispose();
    }
}
