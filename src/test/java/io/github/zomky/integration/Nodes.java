package io.github.zomky.integration;

import io.github.zomky.Node;
import io.github.zomky.NodeFactory;
import io.github.zomky.metrics.MetricsCollector;
import io.github.zomky.metrics.MicrometerMetricsCollector;
import io.github.zomky.raft.FollowerRole;
import io.github.zomky.raft.RaftConfiguration;
import io.github.zomky.raft.RaftGroup;
import io.github.zomky.storage.InMemoryRaftStorage;
import io.github.zomky.storage.RaftStorage;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Nodes {

    private Map<Integer, Node> nodes;

    private Nodes(Map<Integer, Node> nodes) {
        this.nodes = nodes;
    }

    public static Nodes create(int ... ports) {
        CompositeMeterRegistry compositeMeterRegistry = new CompositeMeterRegistry();
        compositeMeterRegistry.add(new SimpleMeterRegistry());
        compositeMeterRegistry.add(new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM));
        MetricsCollector metrics = new MicrometerMetricsCollector(compositeMeterRegistry);

        int joinPort = ports[0];
        Map<Integer, Node> nodes = IntStream.of(ports)
                .mapToObj(port -> NodeFactory.receive()
                        .port(port)
                        .retryJoin(joinPort != port ? joinPort : null)
                        .metrics(metrics)
                        .start()
                        .block())
                .collect(Collectors.toConcurrentMap(Node::getNodeId, n -> n));
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
        Collection<Node> nodesList = (nodeId != null) ? Collections.singletonList(nodes.get(nodeId)) : nodes.values();
        nodesList.forEach(node -> {
            RaftConfiguration raftConfiguration1 = raftConfiguration != null ? raftConfiguration : raftConfigurationFunction.apply(node.getNodeId(), defaultBuilderFunction.apply(node.getNodeId())).build();
            RaftGroup raftGroup = RaftGroup.builder()
                    .groupName(groupName)
                    .raftStorage(raftStorage == null ? new InMemoryRaftStorage() : raftStorage)
                    .raftConfiguration(raftConfiguration1)
                    .cluster(node.getCluster())
                    .raftRole(new FollowerRole())
                    .build();
            node.getRaftProtocol().addGroup(raftGroup);
        });
    }

    public RaftGroup raftGroup(int nodeId, String name) {
        Node innerNode = nodes.get(nodeId);
        return innerNode.getRaftProtocol().getByName(name);
    }

    public boolean isLeader(int nodeId, String name) {
        return raftGroup(nodeId, name).isLeader();
    }

    public void dispose() {
        nodes.values().forEach(Node::dispose);
    }
}
