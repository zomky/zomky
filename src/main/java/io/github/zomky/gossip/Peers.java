package io.github.zomky.gossip;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;

class Peers {

    private static final Logger LOGGER = LoggerFactory.getLogger(Peers.class);

    private int nodeId;
    private Set<Integer> peers = new HashSet<>();
    private BlockingQueue<Integer> shuffledPeers = new LinkedBlockingDeque<>();

    Peers(int nodeId) {
        this.nodeId = nodeId;
    }

    synchronized void add(int peerId) {
        boolean added = peers.add(peerId);
        if (added) {
            LOGGER.info("[Node {}] Adding new peer {}", nodeId, peerId);
            List<Integer> n = new ArrayList<>();
            int r = (int) Math.ceil(Math.random() * shuffledPeers.size());
            shuffledPeers.drainTo(n, r);
            n.add(peerId);
            shuffledPeers.drainTo(n);
            shuffledPeers.addAll(n);
        }
    }

    synchronized void remove(int peerId) {
        LOGGER.info("[Node {}] Removing peer {}", nodeId, peerId);
        peers.remove(peerId);
        shuffledPeers.remove(peerId);
    }

    synchronized PeerProbe nextPeerProbe(int subgroupSize) {
        if (peers.isEmpty()) {
            return PeerProbe.NO_PEER_PROBE;
        }
        final Integer peerId = nextRandomPeerId();
        return new PeerProbe(peerId, selectCompanions(peerId, subgroupSize));
    }

    private List<Integer> selectCompanions(int nodeId, int numberOfCompanions) {
        // TODO improve performance, now is O(n)
        List<Integer> n = new ArrayList<>(peers);
        Collections.shuffle(n);
        return n.stream().filter(i -> i != nodeId).limit(numberOfCompanions).collect(Collectors.toList());
    }

    private Integer nextRandomPeerId() {
        Integer nodeId = shuffledPeers.poll();
        if (nodeId == null) {
            List<Integer> n = new ArrayList<>(peers);
            Collections.shuffle(n);
            shuffledPeers.addAll(n);
            nodeId = shuffledPeers.poll();
        }
        return nodeId;
    }

    int count() {
        return peers.size();
    }
}
