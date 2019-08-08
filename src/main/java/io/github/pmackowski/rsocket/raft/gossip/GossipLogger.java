package io.github.pmackowski.rsocket.raft.gossip;

import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GossipLogger {

    private static final Logger LOGGER = LoggerFactory.getLogger(GossipLogger.class);

    public static void log(Ping ping) {
        if (ping.getDirect()) {
            if (ping.getInitiatorNodeId() != ping.getRequestorNodeId()) {
                LOGGER.info("[Node {}][onPing] Direct probe to {} on behalf of {} successful.", ping.getRequestorNodeId(), ping.getDestinationNodeId(), ping.getInitiatorNodeId());
            } else {
                LOGGER.info("[Node {}][ping] Direct probe to {} successful.", ping.getInitiatorNodeId(), ping.getDestinationNodeId());
            }
        } else {
            LOGGER.info("[Node {}][ping] Indirect probe to {} through {} successful.", ping.getInitiatorNodeId(), ping.getDestinationNodeId(), ping.getRequestorNodeId());
        }
    }

    public static void logError(Ping ping, Throwable throwable) {
        if (ping.getDirect()) {
            if (ping.getInitiatorNodeId() != ping.getRequestorNodeId()) {
                LOGGER.warn("[Node {}][onPing] Direct probe to {} on behalf of {} failed. Reason {}.", ping.getRequestorNodeId(), ping.getDestinationNodeId(), ping.getInitiatorNodeId(), throwable.getMessage());
            } else {
                LOGGER.warn("[Node {}][ping] Direct probe to {} failed. Reason {}.", ping.getInitiatorNodeId(), ping.getDestinationNodeId(), throwable.getMessage());
            }
        } else {
            LOGGER.warn("[Node {}][ping] Indirect probe to {} through {} failed. Reason {}", ping.getInitiatorNodeId(), ping.getDestinationNodeId(), ping.getRequestorNodeId(), throwable.getMessage());
        }
    }
}
