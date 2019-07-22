package io.github.pmackowski.rsocket.raft.raft;

import io.github.pmackowski.rsocket.raft.IntegrationTest;
import io.github.pmackowski.rsocket.raft.Node;
import io.github.pmackowski.rsocket.raft.NodeFactory;
import io.github.pmackowski.rsocket.raft.external.statemachine.KVStateMachine1;
import io.github.pmackowski.rsocket.raft.external.statemachine.KVStateMachineEntryConverter;
import io.github.pmackowski.rsocket.raft.external.statemachine.KVStoreClient;
import io.github.pmackowski.rsocket.raft.external.statemachine.KeyValue;
import io.github.pmackowski.rsocket.raft.storage.meta.Configuration;
import io.rsocket.RSocket;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static io.github.pmackowski.rsocket.raft.gossip.Cluster.DEFAULT_THREE_NODE_CLUSTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
@IntegrationTest
class RaftGroupsTest {
/*
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftGroupsTest.class);

    private static final boolean PRE_VOTE = true;
    private static final boolean LEADER_STICKINESS = true;

    @Mock
    ElectionTimeout electionTimeout1, electionTimeout2, electionTimeout3;

    Node node1, node2, node3, raftServer4;

    @BeforeEach
    public void setUp() {
        BlockHound.builder()
                .allowBlockingCallsInside("java.io.FileInputStream", "readBytes")
                .install();

        node1 = NodeFactory.receive()
                .port(7000)
                .cluster(DEFAULT_THREE_NODE_CLUSTER)
                .start()
                .block();

        node2 = NodeFactory.receive()
                .port(7001)
                .cluster(DEFAULT_THREE_NODE_CLUSTER)
                .start()
                .block();

        node3 = NodeFactory.receive()
                .port(7002)
                .cluster(DEFAULT_THREE_NODE_CLUSTER)
                .start()
                .block();

        RSocket rSocket = NodeFactory.connect()
                .port(7000)
                .start()
                .block();

    }

    @Test
    void testElection() {
        given(electionTimeout1.nextRandom()).willReturn(Duration.ofMillis(300));
        given(electionTimeout2.nextRandom()).willReturn(Duration.ofSeconds(10));
//        given(electionTimeout3.nextRandom()).willReturn(Duration.ofMillis(300));

        RaftConfiguration.Builder templateRaftConfiguration = RaftConfiguration.builder()
            .preVote(PRE_VOTE)
            .leaderStickiness(LEADER_STICKINESS)
            .stateMachineEntryConverter(new KVStateMachineEntryConverter());

        RaftGroup raftServer1Group1 = RaftGroup.builder()
            .groupName("group1")
            .inMemoryRaftStorage()
            .raftConfiguration(templateRaftConfiguration, raftConfiguration -> raftConfiguration
                .stateMachine(new KVStateMachine1(7000))
                .electionTimeout(electionTimeout1)
            )
            .node(node1)
            .configuration(new Configuration(7000,7001))
            .build();

        node1.getRaftGroups().addGroup(raftServer1Group1);

        RaftGroup raftServer2Group1 = RaftGroup.builder()
                .groupName("group1")
                .inMemoryRaftStorage()
                .raftConfiguration(templateRaftConfiguration, raftConfiguration -> raftConfiguration
                        .stateMachine(new KVStateMachine1(7001))
                        .electionTimeout(electionTimeout2)
                )
                .node(node2)
                .configuration(new Configuration(7000,7001))
                .build();

        node2.getRaftGroups().addGroup(raftServer2Group1);


        RaftGroup raftServer3Group2 = RaftGroup.builder()
                .groupName("group2")
                .inMemoryRaftStorage()
                .raftConfiguration(templateRaftConfiguration, raftConfiguration -> raftConfiguration
                        .stateMachine(new KVStateMachine1(7002))
                        .electionTimeout(electionTimeout3)
                )
                .node(node3)
                .configuration(new Configuration(7002))
                .build();

        node3.getRaftGroups().addGroup(raftServer3Group2);

        await().atMost(5, TimeUnit.SECONDS).until(() -> raftServer1Group1.getCurrentLeaderId() == 7000);
        await().atMost(1, TimeUnit.SECONDS).until(() -> raftServer2Group1.getCurrentLeaderId() == 7000);
        await().atMost(1, TimeUnit.SECONDS).until(() -> raftServer3Group2.getCurrentLeaderId() == 7002);

        assertThat(raftServer1Group1.isLeader()).isTrue();
        assertThat(raftServer2Group1.isFollower()).isTrue();
        assertThat(raftServer3Group2.isLeader()).isTrue();
    }

    @Test
    void testLogReplication() {
        testElection();

        KVStoreClient kvStoreClient = new KVStoreClient(7000);

        int nbEntries = 10;

        kvStoreClient.put("group1", Flux.range(1, nbEntries).delayElements(Duration.ofMillis(200)).map(i -> new KeyValue("key" + i, "val" + i)))
                .doOnSubscribe(subscription -> LOGGER.info("KVStoreClient started"))
                .doOnNext(s -> LOGGER.info("KVStoreClient received {}", s))
                .doOnComplete(() -> LOGGER.info("KVStoreClient finished"))
                .blockLast();
    }

    @AfterEach
    void tearDown() {
        node1.dispose();
        node2.dispose();
        node3.dispose();
        if (raftServer4 != null) {
            raftServer4.dispose();
        }
    }
*/
}