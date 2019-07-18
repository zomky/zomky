package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.external.statemachine.KVStateMachine1;
import io.github.pmackowski.rsocket.raft.external.statemachine.KVStateMachineEntryConverter;
import io.github.pmackowski.rsocket.raft.gossip.Cluster;
import io.github.pmackowski.rsocket.raft.raft.*;
import io.github.pmackowski.rsocket.raft.storage.meta.Configuration;

import java.time.Duration;
import java.util.Map;
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

    public void addRaftGroup(String groupName, Configuration configuration, Function<RaftConfiguration.Builder, RaftConfiguration.Builder> raftConfigurationFunction) {
        nodes.values().forEach(node -> {
            RaftGroup raftGroup = RaftGroup.builder()
                    .groupName(groupName)
                    .configuration(configuration)
                    .inMemoryRaftStorage() // TODO hardcoded
                    .node(node)
                    .raftRole(new FollowerRole())
                    .raftConfiguration(RaftConfiguration.builder()
                            .stateMachine(new KVStateMachine1(node.getNodeId())) // TODO hardcoded
                            .stateMachineEntryConverter(new KVStateMachineEntryConverter()) // TODO hardcoded
                            .electionTimeout(ElectionTimeout.defaultTimeout()), raftConfigurationFunction)
                    .build();
            node.getRaftGroups().addGroup(raftGroup);
        });
    }

    public RaftGroup raftGroup(int nodeId, String name) {
        InnerNode innerNode = nodes.get(nodeId);
        return innerNode.getRaftGroups().getByName(name);
    }

    public void dispose() {
        nodes.values().forEach(InnerNode::dispose);
    }
}
