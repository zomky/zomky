package io.github.zomky.gossip;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class Cluster {

    private static final Logger LOGGER = LoggerFactory.getLogger(Cluster.class);

    // temporary
    public static final Cluster DEFAULT_ONE_NODE_CLUSTER   = new Cluster(7000);
    public static final Cluster DEFAULT_TWO_NODE_CLUSTER   = new Cluster(7000, 7001);
    public static final Cluster DEFAULT_THREE_NODE_CLUSTER = new Cluster(7000, 7001, 7002);
    public static final Cluster DEFAULT_FOUR_NODE_CLUSTER  = new Cluster(7000, 7001, 7002, 7003);
    public static final Cluster DEFAULT_FIVE_NODE_CLUSTER  = new Cluster(7000, 7001, 7002, 7003, 7004);

    private Set<Integer> members = new HashSet<>();

    public Cluster(int ... members) {
        for (int m : members) {
            this.members.add(m);
        }
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

    public void addMember(int member) {
//        LOGGER.info("Cluster add member {}", member);
        this.members.add(member);
    }

    public void removeMember(int member) {
        this.members.remove(member);
    }
}
