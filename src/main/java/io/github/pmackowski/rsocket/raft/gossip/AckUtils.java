package io.github.pmackowski.rsocket.raft.gossip;

import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ack;
import io.netty.buffer.Unpooled;
import io.netty.channel.socket.DatagramPacket;

import java.net.InetSocketAddress;

class AckUtils {

    static DatagramPacket toDatagram(Ack ack, InetSocketAddress recipient) {
        return new DatagramPacket(Unpooled.copiedBuffer(ack.toByteArray()), recipient);
    }

    public static Ack nack(Ack ack) {
        return Ack.newBuilder(ack).setNack(true).build();
    }

}
