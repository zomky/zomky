package io.github.pmackowski.rsocket.raft.integration;

import io.github.pmackowski.rsocket.raft.IntegrationTest;
import io.github.pmackowski.rsocket.raft.Node;
import io.github.pmackowski.rsocket.raft.client.ClusterManagementClient;
import io.github.pmackowski.rsocket.raft.external.statemachine.KVStoreClient;
import io.github.pmackowski.rsocket.raft.external.statemachine.KeyValue;
import io.github.pmackowski.rsocket.raft.storage.meta.Configuration;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Map;

@IntegrationTest
class NodeFactoryTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(NodeFactoryTest.class);

    @Test
    void receive() throws InterruptedException {

        Map<Integer, Node> nodes = IntegrationTestsUtils.startNodes(3, 7000);

        //node.dispose();

        ClusterManagementClient clusterManagementClient = new ClusterManagementClient();

        clusterManagementClient.createGroup("group1", 7001, new Configuration(7001,7002)).block();
        clusterManagementClient.createGroup("group2", 7002, new Configuration(7001,7002)).block();


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

    @Test
    void receive2() throws InterruptedException {

        IntegrationTestsUtils.startNodes(3, 7000);

        ClusterManagementClient clusterManagementClient = new ClusterManagementClient();
        clusterManagementClient.createGroup("group1", 7001, new Configuration(7001,7002)).block();

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