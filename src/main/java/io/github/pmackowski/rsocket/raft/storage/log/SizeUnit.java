package io.github.pmackowski.rsocket.raft.storage.log;

public enum SizeUnit {

    bytes(1),
    kilobytes(1024),
    megabytes(1024*1024);

    private int value;

    public int getValue() {
        return value;
    }

    SizeUnit(int value) {
        this.value = value;
    }

}
