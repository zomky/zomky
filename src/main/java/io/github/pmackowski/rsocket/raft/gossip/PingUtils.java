package io.github.pmackowski.rsocket.raft.gossip;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Gossip;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ping;
import io.github.pmackowski.rsocket.raft.utils.NettyUtils;
import io.netty.channel.socket.DatagramPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

class PingUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(PingUtils.class);

    public static Ping toPing(DatagramPacket datagramPacket) {
        try {
            return Ping.parseFrom(NettyUtils.toByteArray(datagramPacket.content().retain()));
        } catch (InvalidProtocolBufferException e) {
            throw new GossipException("datagram packet cannot be converted to Ping", e);
        }
    }

    public static Ping direct(Ping ping) {
        return Ping.newBuilder(ping).setDirect(true).build();
    }

    public static Ping direct(Ping ping, List<Gossip> gossips) {
        return Ping.newBuilder(ping).setDirect(true).clearGossips().addAllGossips(gossips).build();
    }

    public static Ping indirect(Ping ping) {
        return Ping.newBuilder(ping).setDirect(false).build();
    }

}
