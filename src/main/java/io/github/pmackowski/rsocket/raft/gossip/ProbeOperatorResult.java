package io.github.pmackowski.rsocket.raft.gossip;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

class ProbeOperatorResult<T> {

    private boolean indirect;
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

    void add(T t) {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ProbeOperatorResult)) return false;
        ProbeOperatorResult<?> that = (ProbeOperatorResult<?>) o;
        return indirect == that.indirect &&
                Objects.equals(elements, that.elements);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indirect, elements);
    }
}
