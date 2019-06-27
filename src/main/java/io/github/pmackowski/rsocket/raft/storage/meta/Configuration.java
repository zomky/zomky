package io.github.pmackowski.rsocket.raft.storage.meta;

import java.util.HashSet;
import java.util.Set;

public class Configuration {

    public static final Configuration DEFAULT_CONFIGURATION = new Configuration(7000, 7001, 7002);

    private Set<Integer> members = new HashSet<>();

    public Configuration(int ... members) {
        for (int m : members) {
            this.members.add(m);
        }
    }

    public Configuration addMember(int newMember) {
        Configuration configuration = new Configuration();
        this.members.forEach(configuration::addMember);
        configuration.addMember(newMember);
        return configuration;
    }

    public Set<Integer> getMembers() {
        return members;
    }

    public int membersCount() {
        return members.size();
    }
}
