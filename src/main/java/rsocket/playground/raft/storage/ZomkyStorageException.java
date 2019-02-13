package rsocket.playground.raft.storage;

public class ZomkyStorageException extends RuntimeException{

    public ZomkyStorageException(Throwable throwable) {
        super(throwable);
    }

    public ZomkyStorageException(String s) {
        super(s);
    }

    public ZomkyStorageException(String s, Throwable throwable) {
        super(s, throwable);
    }
}
