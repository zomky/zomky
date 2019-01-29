package rsocket.playground.raft;

public class RaftException extends RuntimeException {

    public RaftException(String message) {
        super(message);
    }

    public RaftException(String message, Throwable cause) {
        super(message, cause);
    }

    public RaftException(Throwable cause) {
        super(cause);
    }
}
