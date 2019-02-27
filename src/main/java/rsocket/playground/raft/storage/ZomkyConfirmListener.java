package rsocket.playground.raft.storage;

public interface ZomkyConfirmListener {

    void handle(long index);

}
