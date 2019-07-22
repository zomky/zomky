package io.github.pmackowski.rsocket.raft.gossip;

public enum GossipRpcType {

    PING(1),
    PING_REQ(2);

    private final byte code;

    GossipRpcType(int code) {
        this.code = (byte) code;
    }

    public byte getCode() {
        return code;
    }

    public static GossipRpcType fromCode(int code) {
        switch (code) {
            case 1: return PING;
            case 2: return PING_REQ;

            default: throw new RuntimeException("Unknown gossip type!");
        }
    }

}
