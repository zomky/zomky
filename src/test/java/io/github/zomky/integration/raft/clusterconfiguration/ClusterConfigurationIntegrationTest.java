package io.github.zomky.integration.raft.clusterconfiguration;

import io.github.zomky.IntegrationTest;
import io.github.zomky.integration.Nodes;
import io.github.zomky.external.statemachine.KVStateMachine1;
import io.github.zomky.external.statemachine.KVStateMachineEntryConverter;
import io.github.zomky.integration.IntegrationTestsUtils;
import io.github.zomky.raft.ElectionTimeout;
import io.github.zomky.raft.RaftConfiguration;
import io.github.zomky.storage.RaftStorage;
import io.github.zomky.storage.meta.Configuration;
import io.github.zomky.transport.protobuf.AddServerRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.test.StepVerifier;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@ExtendWith(MockitoExtension.class)
@IntegrationTest
class ClusterConfigurationIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterConfigurationIntegrationTest.class);

    @TempDir
    Path directory;

    RaftStorage raftStorage1, raftStorage2, raftStorage3;

    Nodes nodes;


    @BeforeEach
    void setUp() {
        LOGGER.info("Directory {}", directory);
        IntegrationTestsUtils.checkBlockingCalls();
        raftStorage1 = IntegrationTestsUtils.raftStorage(directory, "1");
        raftStorage2 = IntegrationTestsUtils.raftStorage(directory, "2");
//        raftStorage3 = IntegrationTestsUtils.raftStorage(directory, "3");
    }

    @AfterEach
    void tearDown() {
        raftStorage1.close();
        raftStorage2.close();
//        raftStorage3.close();
        nodes.dispose();
    }

    @Test
    void addServerWhenLogIsEmpty() {
        // given
        nodes = Nodes.create(7000,7001,7002);

        nodes.addRaftGroup(7000, "group1", raftStorage1, this::twoNodeConfiguration, (nodeId,builder) -> builder
                .preVote(false)
                .electionTimeout(ElectionTimeout.exactly(Duration.ofMillis(100)))
        );
        nodes.addRaftGroup(7001, "group1", raftStorage2, this::twoNodeConfiguration, (nodeId,builder) -> builder
                .preVote(false)
                .electionTimeout(ElectionTimeout.exactly(Duration.ofSeconds(10)))
        );
        await().atMost(1, TimeUnit.SECONDS).until(() -> nodes.isLeader(7000, "group1"));

        // when
        StepVerifier.create(nodes.raftGroup(7000, "group1").onAddServer(AddServerRequest.newBuilder().setNewServer(7002).build()))
                    .expectNextCount(1)
                    .verifyComplete();

        // then
        await().atMost(1, TimeUnit.SECONDS).until(() -> raftStorage1.getLastIndexedTerm().getIndex() == 1);
        await().atMost(1, TimeUnit.SECONDS).until(() -> raftStorage2.getLastIndexedTerm().getIndex() == 1);
        await().atMost(1, TimeUnit.SECONDS).until(() -> // TODO
                nodes.raftGroup(7002, "group1").getRaftStorage().getLastIndexedTerm().getIndex() == 1);

    }

    private RaftConfiguration.Builder twoNodeConfiguration(Integer nodeId) {
        return RaftConfiguration.builder()
                .stateMachine(new KVStateMachine1(nodeId))
                .stateMachineEntryConverter(new KVStateMachineEntryConverter())
                .configuration(new Configuration(7000, 7001));
    }

}
