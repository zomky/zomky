package rsocket.playground.raft;

public class LogEntry implements TermAware {

    private long index;
    private long term;
    private String data;

    public LogEntry(long index, long term, String data) {
        this.index = index;
        this.term = term;
        this.data = data;
    }

    @Override
    public long getTerm() {
        return term;
    }

    public long getIndex() {
        return index;
    }

    public String getData() {
        return data;
    }
}
