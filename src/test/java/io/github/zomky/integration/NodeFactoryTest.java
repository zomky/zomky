package io.github.zomky.integration;

import io.github.zomky.IntegrationTest;
import io.github.zomky.client.RaftManagementClient;
import io.github.zomky.external.statemachine.KVStoreClient;
import io.github.zomky.external.statemachine.KeyValue;
import io.github.zomky.storage.meta.Configuration;
import io.github.zomky.transport.protobuf.AddGroupRequest;
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

        Thread.sleep(5_000);

        //node.dispose();

        RaftManagementClient raftManagementClient = new RaftManagementClient();

        Configuration configuration = new Configuration(7001, 7002);

        raftManagementClient.addGroup("group1", addGroupRequest(7001, configuration)).block();
        raftManagementClient.addGroup("group2", addGroupRequest(7002, configuration)).block();

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

//        final AddServerResponse group1 = raftManagementClient.addServer("group1", 7001).block();
//
//        System.out.println(group1);
        Thread.sleep(20_000);

    }

    private AddGroupRequest addGroupRequest(int leaderIdSuggestion, Configuration configuration) {
        return AddGroupRequest.newBuilder()
                .setLeaderIdSuggestion(leaderIdSuggestion)
                .setElectionTimeoutMin(100)
                .setElectionTimeoutMax(1200)
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

        Thread.sleep(5_000);

        RaftManagementClient raftManagementClient = new RaftManagementClient();
        raftManagementClient.addGroup("group1", addGroupRequest(7001, configuration)).block();

        Thread.sleep(3_000);

        raftManagementClient.addServer("group1", 7001, 7000).block();
        //raftManagementClient.removeServer("group1", 7001, 7002).block();
        KVStoreClient kvStoreClient = new KVStoreClient(7001);

        int nbEntries = 10;

        kvStoreClient.put("group1", Flux.range(1, nbEntries).delayElements(Duration.ofMillis(200)).map(i -> new KeyValue("key" + i, "val" + i)))
                .doOnSubscribe(subscription -> LOGGER.info("KVStoreClient started"))
                .doOnNext(s -> LOGGER.info("KVStoreClient received {}", s))
                .doOnComplete(() -> LOGGER.info("KVStoreClient finished"))
                .subscribe();
//        final AddServerResponse group1 = raftManagementClient.addServer("group1", 7001).block();
//
//        System.out.println(group1);
        Thread.sleep(20_000);

    }
}