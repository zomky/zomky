package io.github.pmackowski.rsocket.raft.gossip;

import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ack;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Gossip;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class Gossips {

    private static final Logger LOGGER = LoggerFactory.getLogger(Gossips.class);

    private int nodeId;
    private int incarnation;
    Map<Integer, Gossip> gossips = new ConcurrentHashMap<>();
    // TODO number of times the gossip have been shared already
    // TODO add maximum number of gossips that can be shared in one go

    public Gossips(int nodeId) {
        this.nodeId = nodeId;
    }

    // used when ping on behalf of initiator
    void addAck(Ack ack) {

    }

    // based on probeAcks we should decide about peer condition
    void onProbeCompleted(ProbeAcks probeAcks) {
//        probeAcks.
    }

    List<Gossip> chooseLatestGossipsForPeer() {
        return new ArrayList<>(gossips.values());
    }

    public Ack onPing(int nodeId, Ping ping) {
        List<Gossip> gossipsList = ping.getGossipsList();
        gossipsList.forEach(gossip -> addGossip(gossip.getNodeId(), gossip.getIncarnation(), gossip.getSuspicion()));
        return Ack.newBuilder().setNodeId(nodeId).addAllGossips(chooseLatestGossipsForPeer()).build();
    }

    private synchronized void addGossip(int nodeId, int incarnation, Gossip.Suspicion suspicion) {
        // Node gets gossip about itself
        if (this.nodeId == nodeId) {
            if (suspicion == Gossip.Suspicion.DEAD) {
                LOGGER.error("[Node {}] Oh shit. Node {} says I am dead!", this.nodeId, nodeId);
                addGossipInternal(gossip(this.nodeId, Gossip.Suspicion.DEAD));
                return;
            }
            if (suspicion == Gossip.Suspicion.ALIVE) {
                LOGGER.info("[Node {}] Great! Other node {} think I am alive", this.nodeId, nodeId);
            } else if (suspicion == Gossip.Suspicion.SUSPECT) {
                LOGGER.info("[Node {}] I am suspected by {}!!! C'mon!!", this.nodeId, nodeId);
                if (incarnation == this.incarnation) this.incarnation++;
            }
            addGossipInternal(gossip(this.nodeId, Gossip.Suspicion.ALIVE));
            return;
        }

        Gossip gossip = gossips.get(nodeId);
        if (gossip == null) {
            addGossipInternal(gossip(nodeId, incarnation, suspicion));
            return;
        }

        if (suspicion == Gossip.Suspicion.ALIVE) {
            if (incarnation > gossip.getIncarnation() && (gossip.getSuspicion() == Gossip.Suspicion.ALIVE || gossip.getSuspicion() == Gossip.Suspicion.SUSPECT)) {
                addGossipInternal(gossip(nodeId, incarnation, suspicion));
            }
            return;
        }

        if (suspicion == Gossip.Suspicion.SUSPECT) {
            if ((incarnation > gossip.getIncarnation() && gossip.getSuspicion() == Gossip.Suspicion.SUSPECT) ||
                    (incarnation >= gossip.getIncarnation() && gossip.getSuspicion() == Gossip.Suspicion.ALIVE)) {
                addGossipInternal(gossip(nodeId, incarnation, suspicion));
            }
            return;
        }

        if (suspicion == Gossip.Suspicion.DEAD) {
            addGossipInternal(gossip(nodeId, incarnation, suspicion));
        }
    }

    private Gossip gossip(int nodeId, Gossip.Suspicion suspicion) {
        return Gossip.newBuilder().setNodeId(nodeId).setSuspicion(suspicion).setIncarnation(incarnation).build();
    }

    private Gossip gossip(int nodeId, int incarnation,  Gossip.Suspicion suspicion) {
        return Gossip.newBuilder().setNodeId(nodeId).setIncarnation(incarnation).setSuspicion(suspicion).setIncarnation(incarnation).build();
    }

    private void addGossipInternal(Gossip gossip) {
        LOGGER.error("[Node {}] New gossip [{} is {}, inc: {}]", this.nodeId, gossip.getNodeId(), gossip.getSuspicion(), gossip.getIncarnation());
        gossips.put(nodeId, gossip);
    }

}
