package io.github.pmackowski.rsocket.raft.transport;

public enum RpcType {

    APPEND_ENTRIES(1, true),
    REQUEST_VOTE(2, true),
    PRE_REQUEST_VOTE(3, true),
    ADD_SERVER(4, true),
    REMOVE_SERVER(5, true),
    ADD_GROUP(6, true),

    INFO(10, false),
    JOIN(11, false),
    LEAVE(12, false);

    private final byte code;
    private final boolean raftCommand;

    RpcType(int code, boolean raftCommand) {
        this.code = (byte) code;
        this.raftCommand = raftCommand;
    }

    public byte getCode() {
        return code;
    }

    public boolean isRaftCommand() {
        return raftCommand;
    }

    public static RpcType fromCode(int code) {
        switch (code) {
            case 1: return APPEND_ENTRIES;
            case 2: return REQUEST_VOTE;
            case 3: return PRE_REQUEST_VOTE;
            case 4: return ADD_SERVER;
            case 5: return REMOVE_SERVER;
            case 6: return ADD_GROUP;

            case 10: return INFO;
            case 11: return JOIN;
            case 12: return LEAVE;

            default: throw new RuntimeException("Unknown type !");
        }
    }

}
