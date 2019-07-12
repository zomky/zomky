package io.github.pmackowski.rsocket.raft.integration.clusterconfiguration;

import io.github.pmackowski.rsocket.raft.ElectionTimeout;
import io.github.pmackowski.rsocket.raft.IntegrationTest;
import io.github.pmackowski.rsocket.raft.RaftServer;
import io.github.pmackowski.rsocket.raft.RaftServerBuilder;
import io.github.pmackowski.rsocket.raft.external.statemachine.KVStateMachineEntryConverter;
import io.github.pmackowski.rsocket.raft.integration.IntegrationTestsUtils;
import io.github.pmackowski.rsocket.raft.kvstore.KVStateMachine;
import io.github.pmackowski.rsocket.raft.kvstore.KVStoreClient;
import io.github.pmackowski.rsocket.raft.kvstore.KeyValue;
import io.github.pmackowski.rsocket.raft.transport.protobuf.AddServerRequest;
import io.github.pmackowski.rsocket.raft.storage.RaftStorage;
import io.github.pmackowski.rsocket.raft.storage.meta.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@ExtendWith(MockitoExtension.class)
@IntegrationTest
class ClusterConfigurationIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterConfigurationIntegrationTest.class);

    @TempDir
    Path directory;

    ElectionTimeout electionTimeout = new ElectionTimeout();

    Mono<RaftServer> raftServerMono1, raftServerMono2;
    RaftStorage raftStorage1, raftStorage2;
    RaftServer raftServer1, raftServer2;

    @BeforeEach
    void setUp() {
        LOGGER.info("Directory {}", directory);
        IntegrationTestsUtils.checkBlockingCalls();
        raftStorage1 = IntegrationTestsUtils.raftStorage(directory, "1");
        raftStorage2 = IntegrationTestsUtils.raftStorage(directory, "2");

        raftServerMono1 = new RaftServerBuilder()
                .nodeId(7000)
                .storage(raftStorage1)
                .stateMachine(new KVStateMachine(7000))
                .stateMachineEntryConverter(new KVStateMachineEntryConverter())
                .electionTimeout(electionTimeout)
                .initialConfiguration(new Configuration(7000))
                .start();

        raftServerMono2 = new RaftServerBuilder()
                .nodeId(7001)
                .storage(raftStorage2)
                .stateMachine(new KVStateMachine(7001))
                .stateMachineEntryConverter(new KVStateMachineEntryConverter())
                .electionTimeout(electionTimeout)
                .passive(true)
                .start();

    }

    @AfterEach
    void tearDown() {
        raftStorage1.close();
        raftStorage2.close();
        raftServer1.dispose();
        if (raftServer2 != null)
            raftServer2.dispose();
    }

    @Test
    void testElection() {
        raftServer1 = raftServerMono1.block();
        raftServer2 = raftServerMono2.block();

        await().atMost(2, TimeUnit.SECONDS).until(() -> raftServer1.getCurrentLeaderId() == 7000);

        assertThat(raftServer1.isLeader()).isTrue();

        assertThat(raftStorage1.getTerm()).isEqualTo(1);
        assertThat(raftStorage1.getVotedFor()).isEqualTo(7000);
    }

    @Test
    void testLogReplication() {
        testElection();

        KVStoreClient kvStoreClient = new KVStoreClient(Arrays.asList(7000));
        kvStoreClient.start();

        int nbEntries = 10;

        kvStoreClient.put(Flux.range(1, nbEntries).delayElements(Duration.ofMillis(500)).map(i -> new KeyValue("key" + i, "val" + i)))
                .doOnSubscribe(subscription -> LOGGER.info("KVStoreClient started"))
                .doOnNext(s -> LOGGER.info("KVStoreClient received {}", s))
                .doOnComplete(() -> LOGGER.info("KVStoreClient finished"))
                .blockLast();

        await().atMost(1, TimeUnit.SECONDS).until(() -> raftStorage1.getLastIndexedTerm().getIndex() == nbEntries);
    }

    @Test
    void testLogReplicationWithAddServer() {
        testElection();

        KVStoreClient kvStoreClient = new KVStoreClient(Arrays.asList(7000));
        kvStoreClient.start();

        int nbEntries = 10;

        kvStoreClient.put(Flux.range(1, nbEntries).delayElements(Duration.ofMillis(500)).map(i -> new KeyValue("key" + i, "val" + i)))
                .doOnSubscribe(subscription -> LOGGER.info("KVStoreClient started"))
                .doOnNext(s -> LOGGER.info("KVStoreClient received {}", s))
                .flatMap(keyValue -> {
                    if (keyValue.getKey().equals("key3")) {
                        AddServerRequest addServerRequest = AddServerRequest.newBuilder().setNewServer(7001).build();
                        return raftServer1.onAddServer(addServerRequest);
                    } else {
                        return Flux.empty();
                    }
                })
                .doOnComplete(() -> LOGGER.info("KVStoreClient finished"))
                .blockLast();

        await().atMost(1, TimeUnit.SECONDS).until(() -> raftStorage1.getLastIndexedTerm().getIndex() == nbEntries + 1);

    }

}
