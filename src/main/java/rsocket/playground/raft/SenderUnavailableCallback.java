package rsocket.playground.raft;

public interface SenderUnavailableCallback {

    void handle(Sender sender);

}
