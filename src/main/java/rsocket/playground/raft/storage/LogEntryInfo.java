package rsocket.playground.raft.storage;

import java.util.Objects;

public class LogEntryInfo {

    private long index;
    private int term;

    public LogEntryInfo index(long index) {
        this.index = index;
        return this;
    }

    public LogEntryInfo term(int term) {
        this.term = term;
        return this;
    }

    public long getIndex() {
        return index;
    }

    public int getTerm() {
        return term;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogEntryInfo that = (LogEntryInfo) o;
        return index == that.index &&
                term == that.term;
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, term);
    }

    @Override
    public String toString() {
        return "LogEntryInfo{" +
                "index=" + index +
                ", term=" + term +
                '}';
    }
}
