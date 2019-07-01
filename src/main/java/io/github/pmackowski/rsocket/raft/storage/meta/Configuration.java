package io.github.pmackowski.rsocket.raft.storage.meta;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class Configuration {

    public static final Configuration DEFAULT_CONFIGURATION = new Configuration(7000, 7001, 7002);

    private Set<Integer> members = new HashSet<>();

    public Configuration(Set<Integer> members) {
        this.members.addAll(members);
    }

    private Configuration(int newMember, Set<Integer> members) {
        this.members.addAll(members);
        this.members.add(newMember);
    }

    public Configuration(int ... members) {
        for (int m : members) {
            this.members.add(m);
        }
    }

    public int quorum() {
        return members.size() / 2 + 1;
    }

    public Configuration addMember(int newMember) {
        return new Configuration(newMember, members);
    }

    public Configuration removeMember(int oldMember) {
        return new Configuration(allMembersExcept(oldMember));
    }

    public Set<Integer> getMembers() {
        return members;
    }

    public int membersCount() {
        return members.size();
    }

    public Set<Integer> allMembersExcept(int memberId) {
        return getMembers().stream().filter(member -> !member.equals(memberId)).collect(Collectors.toSet());
    }

    public boolean contains(int nodeId) {
        return members.contains(nodeId);
    }

    @Override
    public String toString() {
        return "Configuration{" +
                "members=" + members +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Configuration that = (Configuration) o;
        return members.equals(that.members);
    }

    @Override
    public int hashCode() {
        return Objects.hash(members);
    }
}
