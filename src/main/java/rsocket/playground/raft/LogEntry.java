package rsocket.playground.raft;

public class LogEntry implements TermAware {

    private long term;
    private String data;

    public LogEntry(long term, String data) {
        this.term = term;
        this.data = data;
    }

    @Override
    public long getTerm() {
        return term;
    }

    public String getData() {
        return data;
    }
}
