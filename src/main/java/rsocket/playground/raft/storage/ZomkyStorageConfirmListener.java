package rsocket.playground.raft.storage;

public interface ZomkyStorageConfirmListener {

    void handle(long index);

}
