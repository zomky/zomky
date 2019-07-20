package io.github.pmackowski.rsocket.raft.integration;

import io.github.pmackowski.rsocket.raft.IntegrationTest;
import io.github.pmackowski.rsocket.raft.Nodes;
import io.github.pmackowski.rsocket.raft.client.ClusterManagementClient;
import io.github.pmackowski.rsocket.raft.external.statemachine.KVStoreClient;
import io.github.pmackowski.rsocket.raft.external.statemachine.KeyValue;
import io.github.pmackowski.rsocket.raft.storage.meta.Configuration;
import io.github.pmackowski.rsocket.raft.transport.protobuf.AddGroupRequest;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.time.Duration;

@IntegrationTest
class NodeFactoryTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(NodeFactoryTest.class);

    @Test
    void receive() throws InterruptedException {

        Nodes nodes = Nodes.create(7000, 7001, 7002);

        //node.dispose();

        ClusterManagementClient clusterManagementClient = new ClusterManagementClient();

        Configuration configuration = new Configuration(7001, 7002);

        clusterManagementClient.addGroup("group1", addGroupRequest(7001, configuration)).block();
        clusterManagementClient.addGroup("group2", addGroupRequest(7002, configuration)).block();

        Thread.sleep(2_000);
        KVStoreClient kvStoreClient = new KVStoreClient(7001);

        int nbEntries = 10;

        kvStoreClient.put("group1", Flux.range(1, nbEntries).delayElements(Duration.ofMillis(200)).map(i -> new KeyValue("key" + i, "val" + i)))
                .doOnSubscribe(subscription -> LOGGER.info("KVStoreClient started"))
                .doOnNext(s -> LOGGER.info("KVStoreClient received {}", s))
                .doOnComplete(() -> LOGGER.info("KVStoreClient finished"))
                .subscribe();

        KVStoreClient kvStoreClient2 = new KVStoreClient(7002);

        kvStoreClient2.put("group2", Flux.range(1, nbEntries).delayElements(Duration.ofMillis(200)).map(i -> new KeyValue("key" + i, "val" + i)))
                .doOnSubscribe(subscription -> LOGGER.info("KVStoreClient2 started"))
                .doOnNext(s -> LOGGER.info("KVStoreClient2 received {}", s))
                .doOnComplete(() -> LOGGER.info("KVStoreClient2 finished"))
                .subscribe();

//        final AddServerResponse group1 = clusterManagementClient.addServer("group1", 7001).block();
//
//        System.out.println(group1);
        Thread.sleep(20_000);

    }

    private AddGroupRequest addGroupRequest(int leaderIdSuggestion, Configuration configuration) {
        return AddGroupRequest.newBuilder()
                .setLeaderIdSuggestion(leaderIdSuggestion)
                .setElectionTimeoutMin(200)
                .setElectionTimeoutMax(400)
                .setPersistentStorage(false)
                .setStateMachine("kv1")
                .addAllNodes(configuration.getMembers())
                .setPassive(false)
                .build();
    }

    @Test
    void receive2() throws InterruptedException {

        Nodes nodes = Nodes.create(7000, 7001, 7002);
        Configuration configuration = new Configuration(7001, 7002);

        ClusterManagementClient clusterManagementClient = new ClusterManagementClient();
        clusterManagementClient.addGroup("group1", addGroupRequest(7001, configuration)).block();

        Thread.sleep(2_000);

        clusterManagementClient.addServer("group1", 7001, 7000).block();
        clusterManagementClient.removeServer("group1", 7001, 7002).block();

        KVStoreClient kvStoreClient = new KVStoreClient(7001);

        int nbEntries = 10;

        kvStoreClient.put("group1", Flux.range(1, nbEntries).delayElements(Duration.ofMillis(200)).map(i -> new KeyValue("key" + i, "val" + i)))
                .doOnSubscribe(subscription -> LOGGER.info("KVStoreClient started"))
                .doOnNext(s -> LOGGER.info("KVStoreClient received {}", s))
                .doOnComplete(() -> LOGGER.info("KVStoreClient finished"))
                .subscribe();

//        final AddServerResponse group1 = clusterManagementClient.addServer("group1", 7001).block();
//
//        System.out.println(group1);
        Thread.sleep(20_000);

    }
}