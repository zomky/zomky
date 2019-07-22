package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.gossip.Cluster;
import io.github.pmackowski.rsocket.raft.raft.FollowerRole;
import io.github.pmackowski.rsocket.raft.raft.RaftConfiguration;
import io.github.pmackowski.rsocket.raft.raft.RaftGroup;
import io.github.pmackowski.rsocket.raft.storage.InMemoryRaftStorage;
import io.github.pmackowski.rsocket.raft.storage.RaftStorage;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Nodes {

    private Map<Integer, InnerNode> nodes;

    private Nodes(Map<Integer, InnerNode> nodes) {
        this.nodes = nodes;
    }

    public static Nodes create(int ... ports) {
        Cluster cluster = new Cluster(ports);
        Map<Integer, InnerNode> nodes = IntStream.of(ports)
                .mapToObj(port -> NodeFactory.receive()
                        .port(port)
                        .cluster(cluster)
                        .start()
                        .block())
                .collect(Collectors.toConcurrentMap(Node::getNodeId, n -> (InnerNode) n));

        return new Nodes(nodes);
    }

    public void addRaftGroup(int nodeId, String groupName, RaftConfiguration raftConfiguration) {
        addRaftGroup(nodeId, groupName, new InMemoryRaftStorage(), raftConfiguration, null, null);
    }

    public void addRaftGroup(int nodeId, String groupName, RaftStorage raftStorage, RaftConfiguration raftConfiguration) {
        addRaftGroup(nodeId, groupName, raftStorage, raftConfiguration, null, null);
    }

    public void addRaftGroup(int nodeId, String groupName, RaftStorage raftStorage, Function<Integer,RaftConfiguration.Builder> defaultBuilderFunction, BiFunction<Integer, RaftConfiguration.Builder, RaftConfiguration.Builder> raftConfigurationFunction) {
        addRaftGroup(nodeId, groupName, raftStorage, null, defaultBuilderFunction, raftConfigurationFunction);
    }

    public void addRaftGroup(int nodeId, String groupName, Function<Integer,RaftConfiguration.Builder> defaultBuilderFunction, BiFunction<Integer, RaftConfiguration.Builder, RaftConfiguration.Builder> raftConfigurationFunction) {
        addRaftGroup(nodeId, groupName, null, null, defaultBuilderFunction, raftConfigurationFunction);
    }

    public void addRaftGroup(String groupName, RaftConfiguration raftConfiguration) {
        addRaftGroup(null, groupName, new InMemoryRaftStorage(), raftConfiguration, null, null);
    }

    public void addRaftGroup(String groupName, RaftStorage raftStorage, RaftConfiguration raftConfiguration) {
        addRaftGroup(null, groupName, raftStorage, raftConfiguration, null, null);
    }

    public void addRaftGroup(String groupName, RaftStorage raftStorage, Function<Integer,RaftConfiguration.Builder> defaultBuilderFunction, BiFunction<Integer, RaftConfiguration.Builder, RaftConfiguration.Builder> raftConfigurationFunction) {
        addRaftGroup(null, groupName, raftStorage, null, defaultBuilderFunction, raftConfigurationFunction);
    }

    public void addRaftGroup(String groupName, Function<Integer,RaftConfiguration.Builder> defaultBuilderFunction, BiFunction<Integer, RaftConfiguration.Builder, RaftConfiguration.Builder> raftConfigurationFunction) {
        addRaftGroup(null, groupName, null, null, defaultBuilderFunction, raftConfigurationFunction);
    }

    private void addRaftGroup(Integer nodeId, String groupName, RaftStorage raftStorage, RaftConfiguration raftConfiguration, Function<Integer, RaftConfiguration.Builder> defaultBuilderFunction, BiFunction<Integer, RaftConfiguration.Builder, RaftConfiguration.Builder> raftConfigurationFunction) {
        Collection<InnerNode> nodesList = (nodeId != null) ? Arrays.asList(nodes.get(nodeId)) : nodes.values();
        nodesList.forEach(node -> {
            RaftConfiguration raftConfiguration1 = raftConfiguration != null ? raftConfiguration : raftConfigurationFunction.apply(node.getNodeId(), defaultBuilderFunction.apply(node.getNodeId())).build();
            RaftGroup raftGroup = RaftGroup.builder()
                    .groupName(groupName)
                    .raftStorage(raftStorage == null ? new InMemoryRaftStorage() : raftStorage)
                    .raftConfiguration(raftConfiguration1)
                    .node(node)
                    .raftRole(new FollowerRole())
                    .build();
            node.getRaftGroups().addGroup(raftGroup);
        });
    }

    public RaftGroup raftGroup(int nodeId, String name) {
        InnerNode innerNode = nodes.get(nodeId);
        return innerNode.getRaftGroups().getByName(name);
    }

    public boolean isLeader(int nodeId, String name) {
        return raftGroup(nodeId, name).isLeader();
    }

    public void dispose() {
        nodes.values().forEach(InnerNode::dispose);
    }
}
