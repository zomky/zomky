package io.github.zomky.storage.log.entry;

public class IndexedTerm {

    private long index;
    private int term;

    public IndexedTerm(long index, int term) {
        this.index = index;
        this.term = term;
    }

    public long getIndex() {
        return index;
    }

    public int getTerm() {
        return term;
    }
}
