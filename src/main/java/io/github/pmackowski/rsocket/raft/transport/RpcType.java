package io.github.pmackowski.rsocket.raft.transport;

public enum RpcType {

    APPEND_ENTRIES(1),
    REQUEST_VOTE(2),
    PRE_REQUEST_VOTE(3),
    ADD_SERVER(4),
    REMOVE_SERVER(5),
    CREATE_GROUP(6),

    INFO(10),

    COMMAND(50);

    private final byte code;

    RpcType(int code) {
        this.code = (byte) code;
    }

    public byte getCode() {
        return code;
    }

    public static RpcType fromCode(int code) {
        switch (code) {
            case 1: return APPEND_ENTRIES;
            case 2: return REQUEST_VOTE;
            case 3: return PRE_REQUEST_VOTE;
            case 4: return ADD_SERVER;
            case 5: return REMOVE_SERVER;
            case 6: return CREATE_GROUP;

            case 10: return INFO;

            case 50: return COMMAND;
            default: throw new RuntimeException("Unknown type !");
        }
    }

}
