package io.github.zomky.integration.raft.election;

import io.github.zomky.IntegrationTest;
import io.github.zomky.Nodes;
import io.github.zomky.external.statemachine.KVStateMachine1;
import io.github.zomky.external.statemachine.KVStateMachineEntryConverter;
import io.github.zomky.integration.IntegrationTestsUtils;
import io.github.zomky.raft.ElectionTimeout;
import io.github.zomky.raft.RaftConfiguration;
import io.github.zomky.raft.RaftGroup;
import io.github.zomky.storage.meta.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@ExtendWith(MockitoExtension.class)
@IntegrationTest
class ElectionThreeNodesIntegrationTest {

    Nodes nodes;

    @BeforeEach
    void setUp() {
        IntegrationTestsUtils.checkBlockingCalls();
    }

    @AfterEach
    void tearDown() {
        nodes.dispose();
    }

    @Test
    void electionPreVoteDisabled() {
        // given
        nodes = Nodes.create(7000,7001,7002);

        // when
        nodes.addRaftGroup(7000, "group1", this::threeNodeConfiguration, (nodeId,builder) -> builder
                .preVote(false)
                .electionTimeout(ElectionTimeout.exactly(Duration.ofMillis(50)))
        );
        nodes.addRaftGroup(7001, "group1", this::threeNodeConfiguration, (nodeId,builder) -> builder
                .preVote(false)
                .electionTimeout(ElectionTimeout.exactly(Duration.ofSeconds(10)))
        );
        nodes.addRaftGroup(7002, "group1", this::threeNodeConfiguration, (nodeId,builder) -> builder
                .preVote(false)
                .electionTimeout(ElectionTimeout.exactly(Duration.ofSeconds(10)))
        );

        // then
        RaftGroup raftGroupNode1 = nodes.raftGroup(7000, "group1");
        RaftGroup raftGroupNode2 = nodes.raftGroup(7001, "group1");
        RaftGroup raftGroupNode3 = nodes.raftGroup(7002, "group1");

        await().atMost(1, TimeUnit.SECONDS).until(() ->
                raftGroupNode1.getCurrentLeaderId() == 7000 &&
                raftGroupNode2.getCurrentLeaderId() == 7000 &&
                raftGroupNode3.getCurrentLeaderId() == 7000
        );

        await().atMost(2, TimeUnit.SECONDS).until(() ->
                raftGroupNode1.isLeader() &&
                raftGroupNode2.isFollower() &&
                raftGroupNode3.isFollower()
        );

        assertThat(raftGroupNode1.getRaftStorage().getTerm()).isGreaterThanOrEqualTo(1);
        assertThat(raftGroupNode2.getRaftStorage().getTerm()).isGreaterThanOrEqualTo(1);
        assertThat(raftGroupNode3.getRaftStorage().getTerm()).isGreaterThanOrEqualTo(1);
        assertThat(raftGroupNode1.getRaftStorage().getTerm()).isEqualTo(raftGroupNode2.getRaftStorage().getTerm());
        assertThat(raftGroupNode2.getRaftStorage().getTerm()).isEqualTo(raftGroupNode3.getRaftStorage().getTerm());

        assertThat(raftGroupNode1.getRaftStorage().getVotedFor()).isEqualTo(7000);
        assertThat(raftGroupNode2.getRaftStorage().getVotedFor() == 7000 || raftGroupNode3.getRaftStorage().getVotedFor() == 7000).isTrue();
    }

    @RepeatedTest(3)
    void electionPreVoteDisabledSameElectionTimeout() {
        // given
        nodes = Nodes.create(7000,7001,7002);

        // when
        nodes.addRaftGroup(7000, "group1", this::threeNodeConfiguration, (nodeId,builder) -> builder
                .preVote(false)
                .electionTimeout(ElectionTimeout.exactly(100))
        );
        nodes.addRaftGroup(7001, "group1", this::threeNodeConfiguration, (nodeId,builder) -> builder
                .preVote(false)
                .electionTimeout(ElectionTimeout.exactly(100))
        );
        nodes.addRaftGroup(7002, "group1", this::threeNodeConfiguration, (nodeId,builder) -> builder
                .preVote(false)
                .electionTimeout(ElectionTimeout.exactly(100))
        );

        // then
        RaftGroup raftGroupNode1 = nodes.raftGroup(7000, "group1");
        RaftGroup raftGroupNode2 = nodes.raftGroup(7001, "group1");
        RaftGroup raftGroupNode3 = nodes.raftGroup(7002, "group1");

        await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> assertAllHasSameLeaderId(raftGroupNode1, raftGroupNode2, raftGroupNode3));
        assertExactlyOneLeader(raftGroupNode1, raftGroupNode2, raftGroupNode3);
        assertVotedFor(raftGroupNode1, raftGroupNode2, raftGroupNode3);
        assertTerm(raftGroupNode1, raftGroupNode2, raftGroupNode3);

    }

    @Test
    void electionPreVoteDisabledOneNodeUnavailable() {
        // given
        nodes = Nodes.create(7000,7001);

        // when
        nodes.addRaftGroup(7000, "group1", this::threeNodeConfiguration, (nodeId,builder) -> builder
                .preVote(false)
                .electionTimeout(ElectionTimeout.exactly(Duration.ofMillis(50)))
        );
        nodes.addRaftGroup(7001, "group1", this::threeNodeConfiguration, (nodeId,builder) -> builder
                .preVote(false)
                .electionTimeout(ElectionTimeout.exactly(Duration.ofSeconds(10)))
        );

        // then
        RaftGroup raftGroupNode1 = nodes.raftGroup(7000, "group1");
        RaftGroup raftGroupNode2 = nodes.raftGroup(7001, "group1");


        await().atMost(1, TimeUnit.SECONDS).until(() ->
                raftGroupNode1.getCurrentLeaderId() == 7000 && raftGroupNode2.getCurrentLeaderId() == 7000
        );

        await().atMost(2, TimeUnit.SECONDS).until(() ->
                raftGroupNode1.isLeader() && raftGroupNode2.isFollower()
        );

        assertThat(raftGroupNode1.getRaftStorage().getTerm()).isGreaterThanOrEqualTo(1);
        assertThat(raftGroupNode2.getRaftStorage().getTerm()).isGreaterThanOrEqualTo(1);
        assertThat(raftGroupNode1.getRaftStorage().getTerm()).isEqualTo(raftGroupNode2.getRaftStorage().getTerm());

        assertThat(raftGroupNode1.getRaftStorage().getVotedFor()).isEqualTo(7000);
        assertThat(raftGroupNode2.getRaftStorage().getVotedFor()).isEqualTo(7000);
    }

    @Test
    void electionPreVoteDisabledTwoNodesUnavailable() throws InterruptedException {
        // given
        nodes = Nodes.create(7000);

        // when
        nodes.addRaftGroup(7000, "group1", this::threeNodeConfiguration, (nodeId,builder) -> builder
                .preVote(false)
                .electionTimeout(ElectionTimeout.exactly(Duration.ofMillis(50)))
        );

        Thread.sleep(1000);

        // then
        RaftGroup raftGroupNode1 = nodes.raftGroup(7000, "group1");

        assertThat(raftGroupNode1.isCandidate()).isTrue();
        assertThat(raftGroupNode1.getRaftStorage().getVotedFor()).isEqualTo(7000);
        assertThat(raftGroupNode1.getRaftStorage().getTerm()).isGreaterThanOrEqualTo(10); // term inflation
    }

    @Test
    void electionPreVoteEnabled() {
        // given
        nodes = Nodes.create(7000,7001,7002);

        // when
        nodes.addRaftGroup(7000, "group1", this::threeNodeConfiguration, (nodeId,builder) -> builder
                .preVote(true)
                .electionTimeout(ElectionTimeout.exactly(Duration.ofMillis(50)))
        );
        nodes.addRaftGroup(7001, "group1", this::threeNodeConfiguration, (nodeId,builder) -> builder
                .preVote(true)
                .electionTimeout(ElectionTimeout.exactly(Duration.ofSeconds(10)))
        );
        nodes.addRaftGroup(7002, "group1", this::threeNodeConfiguration, (nodeId,builder) -> builder
                .preVote(true)
                .electionTimeout(ElectionTimeout.exactly(Duration.ofSeconds(10)))
        );

        // then
        RaftGroup raftGroupNode1 = nodes.raftGroup(7000, "group1");
        RaftGroup raftGroupNode2 = nodes.raftGroup(7001, "group1");
        RaftGroup raftGroupNode3 = nodes.raftGroup(7002, "group1");

        await().atMost(1, TimeUnit.SECONDS).until(() ->
                raftGroupNode1.getCurrentLeaderId() == 7000 &&
                        raftGroupNode2.getCurrentLeaderId() == 7000 &&
                        raftGroupNode3.getCurrentLeaderId() == 7000
        );

        await().atMost(2, TimeUnit.SECONDS).until(() ->
                raftGroupNode1.isLeader() &&
                        raftGroupNode2.isFollower() &&
                        raftGroupNode3.isFollower()
        );

        assertThat(raftGroupNode1.getRaftStorage().getTerm()).isGreaterThanOrEqualTo(1);
        assertThat(raftGroupNode2.getRaftStorage().getTerm()).isGreaterThanOrEqualTo(1);
        assertThat(raftGroupNode3.getRaftStorage().getTerm()).isGreaterThanOrEqualTo(1);
        assertThat(raftGroupNode1.getRaftStorage().getTerm()).isEqualTo(raftGroupNode2.getRaftStorage().getTerm());
        assertThat(raftGroupNode2.getRaftStorage().getTerm()).isEqualTo(raftGroupNode3.getRaftStorage().getTerm());

        assertThat(raftGroupNode1.getRaftStorage().getVotedFor()).isEqualTo(7000);
        assertThat(raftGroupNode2.getRaftStorage().getVotedFor() == 7000 || raftGroupNode3.getRaftStorage().getVotedFor() == 7000).isTrue();
    }

    @Test
    void electionPreVoteEnabledOneNodeUnavailable() {
        // given
        nodes = Nodes.create(7000,7001);

        // when
        nodes.addRaftGroup(7000, "group1", this::threeNodeConfiguration, (nodeId,builder) -> builder
                .preVote(true)
                .electionTimeout(ElectionTimeout.exactly(Duration.ofMillis(50)))
        );
        nodes.addRaftGroup(7001, "group1", this::threeNodeConfiguration, (nodeId,builder) -> builder
                .preVote(true)
                .electionTimeout(ElectionTimeout.exactly(Duration.ofSeconds(10)))
        );

        // then
        RaftGroup raftGroupNode1 = nodes.raftGroup(7000, "group1");
        RaftGroup raftGroupNode2 = nodes.raftGroup(7001, "group1");

        await().atMost(1, TimeUnit.SECONDS).until(() ->
                raftGroupNode1.getCurrentLeaderId() == 7000 && raftGroupNode2.getCurrentLeaderId() == 7000
        );

        await().atMost(2, TimeUnit.SECONDS).until(() ->
                raftGroupNode1.isLeader() && raftGroupNode2.isFollower()
        );

        assertThat(raftGroupNode1.getRaftStorage().getTerm()).isGreaterThanOrEqualTo(1);
        assertThat(raftGroupNode2.getRaftStorage().getTerm()).isGreaterThanOrEqualTo(1);
        assertThat(raftGroupNode1.getRaftStorage().getTerm()).isEqualTo(raftGroupNode2.getRaftStorage().getTerm());

        assertThat(raftGroupNode1.getRaftStorage().getVotedFor()).isEqualTo(7000);
        assertThat(raftGroupNode2.getRaftStorage().getVotedFor() == 7000);
    }

    @Test
    void electionPreVoteEnabledTwoNodesUnavailable() throws InterruptedException {
        // given
        nodes = Nodes.create(7000);

        // when
        nodes.addRaftGroup(7000, "group1", this::threeNodeConfiguration, (nodeId,builder) -> builder
                .preVote(true)
                .electionTimeout(ElectionTimeout.exactly(Duration.ofMillis(50)))
        );

        Thread.sleep(1000);

        // then
        RaftGroup raftGroupNode1 = nodes.raftGroup(7000, "group1");

        assertThat(raftGroupNode1.isFollower()).isTrue();
        assertThat(raftGroupNode1.getRaftStorage().getVotedFor()).isEqualTo(0);
        assertThat(raftGroupNode1.getRaftStorage().getTerm()).isEqualTo(0); // no term inflation
    }

    private RaftConfiguration.Builder threeNodeConfiguration(Integer nodeId) {
        return RaftConfiguration.builder()
                .stateMachine(new KVStateMachine1(nodeId))
                .stateMachineEntryConverter(new KVStateMachineEntryConverter())
                .configuration(new Configuration(7000, 7001, 7002));
    }

    public static void assertExactlyOneLeader(RaftGroup... raftGroups) {
        assertThat(Arrays.asList(raftGroups)).filteredOn(RaftGroup::isLeader).hasSize(1);
        assertThat(Arrays.asList(raftGroups)).filteredOn(RaftGroup::isNotLeader).hasSize(raftGroups.length - 1);
    }

    public static void assertAllHasSameLeaderId(RaftGroup ... raftGroups) {
        Set<Integer> leaderIds = Arrays.stream(raftGroups).map(RaftGroup::getCurrentLeaderId).collect(Collectors.toSet());
        assertThat(leaderIds).hasSize(1).withFailMessage(" leaders %s", raftGroups);
        assertThat(leaderIds.iterator().next()).isGreaterThan(0);
    }

    public static void assertVotedFor(RaftGroup ... raftGroups) {
        await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
            Map<Integer, Long> votedForIds = Arrays.stream(raftGroups)
                    .map(raftGroup -> raftGroup.getRaftStorage().getVotedFor())
                    .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
            // TODO there are different correct possibilities
            long votesForLeader = votedForIds.values().stream().mapToLong(v -> v).max().orElseThrow(NoSuchElementException::new);
            assertThat(votesForLeader).isGreaterThanOrEqualTo(raftGroups.length / 2 + 1); // quorum
        });
    }

    public static void assertTerm(RaftGroup ... raftGroups) {
        await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
            Set<Integer> terms = Arrays.stream(raftGroups).map(raftGroup -> raftGroup.getRaftStorage().getTerm()).collect(Collectors.toSet());
            assertThat(terms).hasSize(1);
            assertThat(terms.iterator().next()).isGreaterThanOrEqualTo(1);
        });
    }

}
