package io.github.pmackowski.rsocket.raft.gossip;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ack;
import io.github.pmackowski.rsocket.raft.utils.NettyUtils;
import io.netty.buffer.Unpooled;
import io.netty.channel.socket.DatagramPacket;

import java.net.InetSocketAddress;

class AckUtils {

    public static Ack toAck(DatagramPacket datagramPacket) {
        try {
            return Ack.parseFrom(NettyUtils.toByteArray(datagramPacket.content().retain()));
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    static DatagramPacket toDatagram(Ack ack, InetSocketAddress recipient) {
        return new DatagramPacket(Unpooled.copiedBuffer(ack.toByteArray()), recipient);
    }
}
