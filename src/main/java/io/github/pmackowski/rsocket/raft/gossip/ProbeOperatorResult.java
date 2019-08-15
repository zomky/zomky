package io.github.pmackowski.rsocket.raft.gossip;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

class ProbeOperatorResult<T> {

    private volatile boolean indirect;
    private AtomicBoolean directSuccessful = new AtomicBoolean(false);
    private List<T> elements = new CopyOnWriteArrayList<>();

    ProbeOperatorResult() {
    }

    @SafeVarargs // for testing
    ProbeOperatorResult(boolean indirect, T ... elements) {
        if (!indirect && elements.length > 1) {
            throw new GossipException("At most one ack is expected when indirect has not been called!");
        }
        this.indirect = indirect;
        this.elements.addAll(Arrays.asList(elements));
    }

    void addDirect(T t) {
        if (directSuccessful.compareAndSet(false, true)) {
            elements.add(t);
        }
    }

    void addIndirect(T t) {
        elements.add(t);
    }

    public void indirect() {
        this.indirect = true;
    }

    List<T> getElements() {
        return elements;
    }

    public boolean isIndirect() {
        return indirect;
    }

    public boolean isDirectSuccessful() {
        return directSuccessful.get();
    }

}
