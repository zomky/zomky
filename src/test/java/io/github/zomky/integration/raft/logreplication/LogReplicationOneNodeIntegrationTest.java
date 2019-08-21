package io.github.zomky.integration.raft.logreplication;

import io.github.zomky.IntegrationTest;
import io.github.zomky.Nodes;
import io.github.zomky.external.statemachine.KVStateMachine1;
import io.github.zomky.external.statemachine.KVStateMachineEntryConverter;
import io.github.zomky.external.statemachine.KVStoreClient;
import io.github.zomky.external.statemachine.KeyValue;
import io.github.zomky.integration.IntegrationTestsUtils;
import io.github.zomky.raft.ElectionTimeout;
import io.github.zomky.raft.RaftConfiguration;
import io.github.zomky.raft.RaftGroup;
import io.github.zomky.storage.RaftStorage;
import io.github.zomky.storage.meta.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
@IntegrationTest
class LogReplicationOneNodeIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogReplicationOneNodeIntegrationTest.class);

    @TempDir
    Path directory;

    RaftStorage raftStorage;

    Nodes nodes;

    @BeforeEach
    void setUp() {
        IntegrationTestsUtils.checkBlockingCalls();
        raftStorage = IntegrationTestsUtils.raftStorage(directory);
        nodes = Nodes.create(7000);
    }

    @AfterEach
    void tearDown() {
        raftStorage.close();
        nodes.dispose();
    }

    @Test
    void logReplication() {
        // when
        nodes.addRaftGroup("group1", raftStorage, raftConfiguration());
        KVStoreClient kvStoreClient = new KVStoreClient(7000);
        int nbEntries = 10;

        // then
        StepVerifier.create(kvStoreClient.put("group1", Flux.range(1, nbEntries).map(i ->
                new KeyValue("key" + i, "val" + i)))
                .doOnSubscribe(subscription -> LOGGER.warn("KVStoreClient started"))
                .doOnNext(s -> LOGGER.info("KVStoreClient received {}", s))
                .doOnComplete(() -> LOGGER.warn("KVStoreClient finished")))
                .expectNextCount(nbEntries)
                .verifyComplete();

        // then
        RaftGroup actual = nodes.raftGroup(7000, "group1");

        assertThat(actual.isLeader()).isTrue();
        assertThat(raftStorage.getTerm()).isEqualTo(1);
        assertThat(raftStorage.getVotedFor()).isEqualTo(7000);
        assertThat(raftStorage.getLastIndexedTerm().getIndex()).isEqualTo(nbEntries);
    }

    private RaftConfiguration raftConfiguration() {
        return RaftConfiguration.builder()
                .stateMachine(new KVStateMachine1(7000))
                .stateMachineEntryConverter(new KVStateMachineEntryConverter())
                .electionTimeout(ElectionTimeout.defaultTimeout())
                .configuration(new Configuration(7000))
                .preVote(true)
                .build();
    }

}
