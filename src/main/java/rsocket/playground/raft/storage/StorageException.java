package rsocket.playground.raft.storage;

class StorageException extends RuntimeException {

    public StorageException(Throwable throwable) {
        super(throwable);
    }

    public StorageException(String s, Throwable throwable) {
        super(s, throwable);
    }
}
