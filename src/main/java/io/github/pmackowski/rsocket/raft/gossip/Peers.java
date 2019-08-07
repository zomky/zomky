package io.github.pmackowski.rsocket.raft.gossip;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;

class Peers {

    private List<Integer> peers = new ArrayList<>();
    private BlockingQueue<Integer> shuffledPeers = new LinkedBlockingDeque<>();

    public Peers() {}

    public Peers(List<Integer> peers) {
        this.peers = peers;
    }

    public void add(int nodeId) {
        peers.add(nodeId);
        List<Integer> n = new ArrayList<>();
        int r = (int) Math.ceil(Math.random() * shuffledPeers.size());
        shuffledPeers.drainTo(n, r);
        n.add(nodeId);
        shuffledPeers.drainTo(n);
    }

    public void remove(int nodeId) {
        peers.remove(nodeId);
    }

    public List<Integer> selectCompanions(int nodeId, int numberOfCompanions) {
        // TODO improve performance, now is O(n)
        List<Integer> n = new ArrayList<>(peers);
        Collections.shuffle(n);
        return n.stream().filter(i -> i != nodeId).limit(numberOfCompanions).collect(Collectors.toList());
    }

    public Integer nextRandomPeerId() {
        Integer nodeId = shuffledPeers.poll();
        if (nodeId == null) {
            List<Integer> n = new ArrayList<>(peers);
            Collections.shuffle(n);
            shuffledPeers.addAll(n);
            nodeId = shuffledPeers.poll();
        }
        return nodeId;
    }

}
