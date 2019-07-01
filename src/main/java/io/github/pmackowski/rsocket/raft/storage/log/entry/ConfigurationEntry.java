package io.github.pmackowski.rsocket.raft.storage.log.entry;

import java.util.HashSet;
import java.util.Set;

public class ConfigurationEntry extends LogEntry {

    private Set<Integer> members = new HashSet<>();

    public ConfigurationEntry(int term, long timestamp, Set<Integer> members) {
        super(term, timestamp);
        this.members.addAll(members);
    }

    public Set<Integer> getMembers() {
        return members;
    }
}
