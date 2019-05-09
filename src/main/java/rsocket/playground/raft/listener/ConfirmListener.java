package rsocket.playground.raft.listener;

public interface ConfirmListener {

    void handle(long index);

}
