package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.external.statemachine.KVStateMachineEntryConverter;
import io.github.pmackowski.rsocket.raft.kvstore.KVStateMachine;
import io.github.pmackowski.rsocket.raft.storage.InMemoryRaftStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
@IntegrationTest
class RaftGroupTest {
/*
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftGroupTest.class);

    private static final boolean PRE_VOTE = false; // TODO add new tests for pre vote
    private static final boolean LEADER_STICKINESS = false; // TODO add new tests for leader stickiness


    @Mock
    ElectionTimeout electionTimeout1, electionTimeout2, electionTimeout3, electionTimeout4;

    Mono<Node> raftServerMono1, raftServerMono2, raftServerMono3, raftServerMono4;
    Node node1, node2, node3, raftServer4;

    @BeforeEach
    public void setUp() {
        BlockHound.builder()
                .allowBlockingCallsInside("java.io.FileInputStream", "readBytes")
                .install();

        raftServerMono1 = new NodeFactory()
                .nodeId(7000)
                .storage(new InMemoryRaftStorage())
                .stateMachine(new KVStateMachine(7000))
                .stateMachineEntryConverter(new KVStateMachineEntryConverter())
                .electionTimeout(electionTimeout1)
                .preVote(PRE_VOTE)
                .leaderStickiness(LEADER_STICKINESS)
                .start();
        raftServerMono2 = new NodeFactory()
                .nodeId(7001)
                .storage(new InMemoryRaftStorage())
                .stateMachine(new KVStateMachine(7001))
                .stateMachineEntryConverter(new KVStateMachineEntryConverter())
                .electionTimeout(electionTimeout2)
                .preVote(PRE_VOTE)
                .leaderStickiness(LEADER_STICKINESS)
                .start();
        raftServerMono3 = new NodeFactory()
                .nodeId(7002)
                .storage(new InMemoryRaftStorage())
                .stateMachine(new KVStateMachine(7002))
                .stateMachineEntryConverter(new KVStateMachineEntryConverter())
                .electionTimeout(electionTimeout3)
                .preVote(PRE_VOTE)
                .leaderStickiness(LEADER_STICKINESS)
                .start();
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

    @Test
    void testElection() {
        given(electionTimeout1.nextRandom()).willReturn(Duration.ofMillis(300));
        given(electionTimeout2.nextRandom()).willReturn(Duration.ofSeconds(10));
        given(electionTimeout3.nextRandom()).willReturn(Duration.ofSeconds(10));

        node1 = raftServerMono1.block();
        node2 = raftServerMono2.block();
        node3 = raftServerMono3.block();

        final RaftGroup raftServer1Group1 = new RaftGroup(new InMemoryRaftStorage(), (DefaultNode) node1, "group1", Arrays.asList(7000, 7001));
        node1.addGroup(raftServer1Group1);
        final RaftGroup raftServer2Group1 = new RaftGroup(new InMemoryRaftStorage(), (DefaultNode) node2, "group1", Arrays.asList(7000, 7001));
        node2.addGroup(raftServer2Group1);
        final RaftGroup raftServer3Group1 = new RaftGroup(new InMemoryRaftStorage(), (DefaultNode) node3, "group1", Arrays.asList(7000, 7001));
        node3.addGroup(raftServer3Group1);

        ((DefaultNode) node1).startGroups();
        ((DefaultNode) node2).startGroups();
        ((DefaultNode) node3).startGroups();

        await().atMost(5, TimeUnit.SECONDS).until(() -> raftServer1Group1.getCurrentLeaderId() == 7000);
        await().atMost(1, TimeUnit.SECONDS).until(() -> raftServer2Group1.getCurrentLeaderId() == 7000);
        await().atMost(1, TimeUnit.SECONDS).until(() -> raftServer3Group1.getCurrentLeaderId() == 7000);

        assertThat(raftServer1Group1.isLeader()).isTrue();
        assertThat(raftServer2Group1.isFollower()).isTrue();
        assertThat(raftServer3Group1.isFollower()).isTrue();
    }
*/
}