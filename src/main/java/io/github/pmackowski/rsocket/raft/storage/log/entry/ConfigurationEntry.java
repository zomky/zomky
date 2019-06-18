package io.github.pmackowski.rsocket.raft.storage.log.entry;

import java.util.Set;

public class ConfigurationEntry extends LogEntry {

    private Set<Integer> members;

    public ConfigurationEntry(int term, long timestamp, Set<Integer> members) {
        super(term, timestamp);
        this.members = members;
    }

    public Set<Integer> getMembers() {
        return members;
    }
}
