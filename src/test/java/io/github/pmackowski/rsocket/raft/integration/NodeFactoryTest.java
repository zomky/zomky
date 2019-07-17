package io.github.pmackowski.rsocket.raft.integration;

import io.github.pmackowski.rsocket.raft.IntegrationTest;
import io.github.pmackowski.rsocket.raft.Node;
import io.github.pmackowski.rsocket.raft.NodeFactory;
import io.github.pmackowski.rsocket.raft.client.ClusterManagementClient;
import io.github.pmackowski.rsocket.raft.gossip.Cluster;
import io.github.pmackowski.rsocket.raft.raft.RaftGroup;
import io.github.pmackowski.rsocket.raft.raft.RaftGroups;
import io.github.pmackowski.rsocket.raft.storage.meta.Configuration;
import io.github.pmackowski.rsocket.raft.transport.protobuf.AddServerResponse;
import org.junit.jupiter.api.Test;

@IntegrationTest
class NodeFactoryTest {

    @Test
    void receive() {

        Node node = NodeFactory.receive()
                .port(7000)
                .cluster(new Cluster(7000, 7001))
                .start()
                .block();

        Node node2 = NodeFactory.receive()
                .port(7001)
                .cluster(new Cluster(7000,7001))
                .start()
                .block();

        RaftGroups raftGroups = node.getRaftGroups();
        raftGroups.addGroup(RaftGroup.builder()
                .groupName("group1")
                .node(node)
                .inMemoryRaftStorage()
                .configuration(new Configuration(7000))
                .build()
        );

        RaftGroups raftGroups2 = node2.getRaftGroups();
        raftGroups2.addGroup(RaftGroup.builder()
                .groupName("group1")
                .node(node2)
                .inMemoryRaftStorage()
                .configuration(new Configuration(7001))
                .build()
        );

        //node.dispose();

        ClusterManagementClient clusterManagementClient = new ClusterManagementClient(7000);

        final AddServerResponse group1 = clusterManagementClient.addServer("group1", 7001).block();

        System.out.println(group1);

    }
}