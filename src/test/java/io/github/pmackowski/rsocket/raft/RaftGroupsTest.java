package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.external.statemachine.KVStateMachine1;
import io.github.pmackowski.rsocket.raft.external.statemachine.KVStateMachineEntryConverter;
import io.github.pmackowski.rsocket.raft.external.statemachine.KVStoreClient;
import io.github.pmackowski.rsocket.raft.external.statemachine.KeyValue;
import io.github.pmackowski.rsocket.raft.storage.InMemoryRaftStorage;
import io.github.pmackowski.rsocket.raft.storage.meta.Configuration;
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
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static io.github.pmackowski.rsocket.raft.gossip.Cluster.DEFAULT_THREE_NODE_CLUSTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
@IntegrationTest
class RaftGroupsTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftGroupsTest.class);

    private static final boolean PRE_VOTE = true;
    private static final boolean LEADER_STICKINESS = true;

    @Mock
    ElectionTimeout electionTimeout1, electionTimeout2, electionTimeout3;

    Mono<Node> raftServerMono1, raftServerMono2, raftServerMono3;
    Node raftServer1, raftServer2, raftServer3, raftServer4;

    @BeforeEach
    public void setUp() {
        BlockHound.builder()
                .allowBlockingCallsInside("java.io.FileInputStream", "readBytes")
                .install();

        raftServerMono1 = new NodeBuilder()
                .nodeId(7000)
                .cluster(DEFAULT_THREE_NODE_CLUSTER)
                .start();

        raftServerMono2 = new NodeBuilder()
                .nodeId(7001)
                .cluster(DEFAULT_THREE_NODE_CLUSTER)
                .start();

        raftServerMono3 = new NodeBuilder()
                .nodeId(7002)
                .cluster(DEFAULT_THREE_NODE_CLUSTER)
                .start();
    }

    @Test
    void testElection() {
        given(electionTimeout1.nextRandom()).willReturn(Duration.ofMillis(300));
        given(electionTimeout2.nextRandom()).willReturn(Duration.ofSeconds(10));
//        given(electionTimeout3.nextRandom()).willReturn(Duration.ofMillis(300));

        raftServer1 = raftServerMono1.block();
        raftServer2 = raftServerMono2.block();
        raftServer3 = raftServerMono3.block();

        raftServer1.getRaftGroups();


        RaftConfiguration raftConfiguration1 = RaftConfiguration.builder()
                .preVote(PRE_VOTE)
                .leaderStickiness(LEADER_STICKINESS)
                .electionTimeout(electionTimeout1)
                .stateMachine(new KVStateMachine1(7000))
                .stateMachineEntryConverter(new KVStateMachineEntryConverter())
                .build();
        final RaftGroup raftServer1Group1 = new RaftGroup(new InMemoryRaftStorage(), raftConfiguration1, (InnerNode) raftServer1, "group1", new Configuration(7000,7001));
        raftServer1.getRaftGroups().addGroup(raftServer1Group1);

        RaftConfiguration raftConfiguration2 = RaftConfiguration.builder()
                .preVote(PRE_VOTE)
                .leaderStickiness(LEADER_STICKINESS)
                .electionTimeout(electionTimeout2)
                .stateMachine(new KVStateMachine1(7001))
                .stateMachineEntryConverter(new KVStateMachineEntryConverter())
                .build();
        final RaftGroup raftServer2Group1 = new RaftGroup(new InMemoryRaftStorage(), raftConfiguration2, (DefaultNode) raftServer2, "group1", new Configuration(7000,7001));
        raftServer2.getRaftGroups().addGroup(raftServer2Group1);


        RaftConfiguration raftConfiguration3 = RaftConfiguration.builder()
                .preVote(PRE_VOTE)
                .leaderStickiness(LEADER_STICKINESS)
                .electionTimeout(electionTimeout3)
                .stateMachine(new KVStateMachine1(7002))
                .stateMachineEntryConverter(new KVStateMachineEntryConverter())
                .build();
        final RaftGroup raftServer3Group2 = new RaftGroup(new InMemoryRaftStorage(), raftConfiguration3, (DefaultNode) raftServer3, "group2", new Configuration(7002) );
        raftServer3.getRaftGroups().addGroup(raftServer3Group2);

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
        raftServer1.dispose();
        raftServer2.dispose();
        raftServer3.dispose();
        if (raftServer4 != null) {
            raftServer4.dispose();
        }
    }
}