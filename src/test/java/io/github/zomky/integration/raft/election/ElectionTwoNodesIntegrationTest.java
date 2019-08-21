package io.github.zomky.integration.raft.election;

import io.github.zomky.IntegrationTest;
import io.github.zomky.Nodes;
import io.github.zomky.external.statemachine.KVStateMachine1;
import io.github.zomky.external.statemachine.KVStateMachineEntryConverter;
import io.github.zomky.integration.IntegrationTestsUtils;
import io.github.zomky.raft.ElectionTimeout;
import io.github.zomky.raft.RaftConfiguration;
import io.github.zomky.raft.RaftGroup;
import io.github.zomky.storage.InMemoryRaftStorage;
import io.github.zomky.storage.RaftStorage;
import io.github.zomky.storage.log.entry.CommandEntry;
import io.github.zomky.storage.meta.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@ExtendWith(MockitoExtension.class)
@IntegrationTest
class ElectionTwoNodesIntegrationTest {

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
        nodes = Nodes.create(7000,7001);

        // when
        nodes.addRaftGroup(7000, "group1", this::twoNodeConfiguration, (nodeId,builder) -> builder
                .preVote(false)
                .electionTimeout(ElectionTimeout.exactly(Duration.ofMillis(50)))
        );
        nodes.addRaftGroup(7001, "group1", this::twoNodeConfiguration, (nodeId,builder) -> builder
                .preVote(false)
                .electionTimeout(ElectionTimeout.exactly(Duration.ofSeconds(10)))
        );

        // then
        RaftGroup raftGroupNode1 = nodes.raftGroup(7000, "group1");
        RaftGroup raftGroupNode2 = nodes.raftGroup(7001, "group1");

        await().atMost(1, TimeUnit.SECONDS).until(() -> raftGroupNode1.getCurrentLeaderId() == 7000);
        await().atMost(1, TimeUnit.SECONDS).until(() -> raftGroupNode2.getCurrentLeaderId() == 7000);

        assertThat(raftGroupNode1.isLeader()).isTrue();
        assertThat(raftGroupNode2.isFollower()).isTrue();

        assertThat(raftGroupNode1.getRaftStorage().getTerm()).isGreaterThanOrEqualTo(1);
        assertThat(raftGroupNode2.getRaftStorage().getTerm()).isGreaterThanOrEqualTo(1);
        assertThat(raftGroupNode1.getRaftStorage().getTerm()).isEqualTo(raftGroupNode2.getRaftStorage().getTerm());

        assertThat(raftGroupNode1.getRaftStorage().getVotedFor()).isEqualTo(7000);
        assertThat(raftGroupNode2.getRaftStorage().getVotedFor()).isEqualTo(7000);
    }

    @RepeatedTest(3)
    void electionPreVoteDisabledSameElectionTimeout() {
        // given
        nodes = Nodes.create(7000,7001);

        // when
        nodes.addRaftGroup(7000, "group1", this::twoNodeConfiguration, (nodeId,builder) -> builder
                .preVote(false)
                .electionTimeout(ElectionTimeout.exactly(100))
        );
        nodes.addRaftGroup(7001, "group1", this::twoNodeConfiguration, (nodeId,builder) -> builder
                .preVote(false)
                .electionTimeout(ElectionTimeout.exactly(100))
        );

        // then
        RaftGroup raftGroupNode1 = nodes.raftGroup(7000, "group1");
        RaftGroup raftGroupNode2 = nodes.raftGroup(7001, "group1");

        await().atMost(50, TimeUnit.SECONDS).untilAsserted(() -> assertAllHasSameLeaderId(raftGroupNode1, raftGroupNode2));
        assertExactlyOneLeader(raftGroupNode1, raftGroupNode2);
        assertVotedFor(raftGroupNode1, raftGroupNode2);
        assertTerm(raftGroupNode1, raftGroupNode2);

    }

    @Test
    void electionPreVoteDisabledOneNodeUnavailable() throws InterruptedException {
        // given
        nodes = Nodes.create(7000);

        // when
        nodes.addRaftGroup(7000, "group1", this::twoNodeConfiguration, (nodeId,builder) -> builder
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
    void electionPreVoteDisabledGroupAddedLater() throws InterruptedException {
        // given
        nodes = Nodes.create(7000,7001);

        // when
        nodes.addRaftGroup(7000, "group1", this::twoNodeConfiguration, (nodeId,builder) -> builder
                .preVote(false)
                .electionTimeout(ElectionTimeout.exactly(Duration.ofMillis(50)))
        );

        Thread.sleep(200);
        nodes.addRaftGroup(7001, "group1", this::twoNodeConfiguration, (nodeId,builder) -> builder
                .preVote(false)
                .electionTimeout(ElectionTimeout.exactly(Duration.ofSeconds(10)))
        );

        // then
        RaftGroup raftGroupNode1 = nodes.raftGroup(7000, "group1");
        RaftGroup raftGroupNode2 = nodes.raftGroup(7001, "group1");

        await().atMost(1, TimeUnit.SECONDS).until(() -> raftGroupNode1.getCurrentLeaderId() == 7000);
        await().atMost(1, TimeUnit.SECONDS).until(() -> raftGroupNode2.getCurrentLeaderId() == 7000);

        assertThat(raftGroupNode1.isLeader()).isTrue();
        assertThat(raftGroupNode2.isFollower()).isTrue();

        assertThat(raftGroupNode1.getRaftStorage().getTerm()).isGreaterThanOrEqualTo(1);
        assertThat(raftGroupNode2.getRaftStorage().getTerm()).isGreaterThanOrEqualTo(1);
        assertThat(raftGroupNode1.getRaftStorage().getTerm()).isEqualTo(raftGroupNode2.getRaftStorage().getTerm());

        assertThat(raftGroupNode1.getRaftStorage().getVotedFor()).isEqualTo(7000);
        assertThat(raftGroupNode2.getRaftStorage().getVotedFor()).isEqualTo(7000);
    }

    @Test
    void electionPreVoteDisabledLowerTerm() {
        // given
        nodes = Nodes.create(7000, 7001);
        RaftStorage node1Storage = new InMemoryRaftStorage();
        RaftStorage node2Storage = new InMemoryRaftStorage();
        node2Storage.update(1000, 7001);

        // when
        nodes.addRaftGroup(7000, "group1", node1Storage, this::twoNodeConfiguration, (nodeId,builder) -> builder
                .preVote(false)
                .electionTimeout(ElectionTimeout.exactly(Duration.ofMillis(300)))
        );
        nodes.addRaftGroup(7001, "group1", node2Storage, this::twoNodeConfiguration, (nodeId,builder) -> builder
                .preVote(false)
                .electionTimeout(ElectionTimeout.exactly(Duration.ofSeconds(10)))
        );

        // then
        RaftGroup raftGroupNode1 = nodes.raftGroup(7000, "group1");
        RaftGroup raftGroupNode2 = nodes.raftGroup(7001, "group1");

        await().atMost(1, TimeUnit.SECONDS).until(() -> raftGroupNode1.getCurrentLeaderId() == 7000);
        await().atMost(1, TimeUnit.SECONDS).until(() -> raftGroupNode2.getCurrentLeaderId() == 7000);

        assertThat(raftGroupNode1.isLeader()).isTrue();
        assertThat(raftGroupNode2.isFollower()).isTrue();

        assertThat(node1Storage.getTerm()).isGreaterThanOrEqualTo(1001);
        assertThat(node2Storage.getTerm()).isGreaterThanOrEqualTo(1001);
        assertThat(node1Storage.getTerm()).isEqualTo(node2Storage.getTerm());

        assertThat(node1Storage.getVotedFor()).isEqualTo(7000);
        assertThat(node2Storage.getVotedFor()).isEqualTo(7000);
    }

    @Test
    void electionPreVoteDisabledLogNotUpToDate() {
        // given
        nodes = Nodes.create(7000, 7001);
        RaftStorage node1Storage = new InMemoryRaftStorage();
        RaftStorage node2Storage = new InMemoryRaftStorage();
        node2Storage.update(1000, 7001);
        node2Storage.append(new CommandEntry(1000, System.currentTimeMillis(), "abc".getBytes()));

        // when
        nodes.addRaftGroup(7000, "group1", node1Storage, this::twoNodeConfiguration, (nodeId,builder) -> builder
                .preVote(false)
                .electionTimeout(ElectionTimeout.exactly(Duration.ofMillis(300)))
        );
        nodes.addRaftGroup(7001, "group1", node2Storage, this::twoNodeConfiguration, (nodeId,builder) -> builder
                .preVote(false)
                .electionTimeout(ElectionTimeout.exactly(Duration.ofSeconds(1)))
        );

        // then
        RaftGroup raftGroupNode1 = nodes.raftGroup(7000, "group1");
        RaftGroup raftGroupNode2 = nodes.raftGroup(7001, "group1");

        await().atMost(2, TimeUnit.SECONDS).until(() -> raftGroupNode1.getCurrentLeaderId() == 7001);
        await().atMost(1, TimeUnit.SECONDS).until(() -> raftGroupNode2.getCurrentLeaderId() == 7001);

        assertThat(raftGroupNode1.isFollower()).isTrue();
        assertThat(raftGroupNode2.isLeader()).isTrue();

        assertThat(node1Storage.getTerm()).isGreaterThanOrEqualTo(1001);
        assertThat(node2Storage.getTerm()).isGreaterThanOrEqualTo(1001);
        assertThat(node1Storage.getTerm()).isEqualTo(node2Storage.getTerm());

        assertThat(node1Storage.getVotedFor()).isEqualTo(7001);
        assertThat(node2Storage.getVotedFor()).isEqualTo(7001);

    }

    @Test
    void electionPreVoteEnabled() {
        // given
        nodes = Nodes.create(7000,7001);

        // when
        nodes.addRaftGroup(7000, "group1", this::twoNodeConfiguration, (nodeId,builder) -> builder
                .preVote(true)
                .electionTimeout(ElectionTimeout.exactly(Duration.ofMillis(100)))
        );
        nodes.addRaftGroup(7001, "group1", this::twoNodeConfiguration, (nodeId,builder) -> builder
                .preVote(true)
                .electionTimeout(ElectionTimeout.exactly(Duration.ofSeconds(10)))
        );

        // then
        RaftGroup raftGroupNode1 = nodes.raftGroup(7000, "group1");
        RaftGroup raftGroupNode2 = nodes.raftGroup(7001, "group1");

        await().atMost(1, TimeUnit.SECONDS).until(() -> raftGroupNode1.getCurrentLeaderId() == 7000);
        await().atMost(1, TimeUnit.SECONDS).until(() -> raftGroupNode2.getCurrentLeaderId() == 7000);

        assertThat(raftGroupNode1.isLeader()).isTrue();
        assertThat(raftGroupNode2.isFollower()).isTrue();

        assertThat(raftGroupNode1.getRaftStorage().getTerm()).isEqualTo(1);
        assertThat(raftGroupNode2.getRaftStorage().getTerm()).isEqualTo(1);

        assertThat(raftGroupNode1.getRaftStorage().getVotedFor()).isEqualTo(7000);
        assertThat(raftGroupNode2.getRaftStorage().getVotedFor()).isEqualTo(7000);
    }

    @Test
    void electionPreVoteEnabledOneNodeUnavailable() throws InterruptedException {
        // given
        nodes = Nodes.create(7000);

        // when
        nodes.addRaftGroup(7000, "group1", this::twoNodeConfiguration, (nodeId,builder) -> builder
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

    @Test
    void electionPreVoteEnabledLogNotUpToDate() {
        // given
        nodes = Nodes.create(7000, 7001);
        RaftStorage node1Storage = new InMemoryRaftStorage();
        RaftStorage node2Storage = new InMemoryRaftStorage();
        node2Storage.update(1000, 7001);
        node2Storage.append(new CommandEntry(1000, System.currentTimeMillis(), "abc".getBytes()));

        // when
        nodes.addRaftGroup(7000, "group1", node1Storage, this::twoNodeConfiguration, (nodeId,builder) -> builder
                .preVote(true)
                .electionTimeout(ElectionTimeout.exactly(Duration.ofMillis(300)))
        );
        nodes.addRaftGroup(7001, "group1", node2Storage, this::twoNodeConfiguration, (nodeId,builder) -> builder
                .preVote(true)
                .electionTimeout(ElectionTimeout.exactly(Duration.ofSeconds(1)))
        );

        // then
        RaftGroup raftGroupNode1 = nodes.raftGroup(7000, "group1");
        RaftGroup raftGroupNode2 = nodes.raftGroup(7001, "group1");

        await().atMost(2, TimeUnit.SECONDS).until(() -> raftGroupNode1.getCurrentLeaderId() == 7001);
        await().atMost(1, TimeUnit.SECONDS).until(() -> raftGroupNode2.getCurrentLeaderId() == 7001);

        assertThat(raftGroupNode1.isFollower()).isTrue();
        assertThat(raftGroupNode2.isLeader()).isTrue();

        assertThat(node1Storage.getTerm()).isGreaterThanOrEqualTo(1001);
        assertThat(node2Storage.getTerm()).isGreaterThanOrEqualTo(1001);
        assertThat(node1Storage.getTerm()).isEqualTo(node2Storage.getTerm());

        assertThat(node1Storage.getVotedFor()).isEqualTo(7001);
        assertThat(node2Storage.getVotedFor()).isEqualTo(7001);

    }

    private RaftConfiguration.Builder twoNodeConfiguration(Integer nodeId) {
        return RaftConfiguration.builder()
                .stateMachine(new KVStateMachine1(nodeId))
                .stateMachineEntryConverter(new KVStateMachineEntryConverter())
                .configuration(new Configuration(7000, 7001));
    }

    public static void assertExactlyOneLeader(RaftGroup... raftGroups) {
        assertThat(Arrays.asList(raftGroups)).filteredOn(RaftGroup::isLeader).hasSize(1);
        assertThat(Arrays.asList(raftGroups)).filteredOn(RaftGroup::isNotLeader).hasSize(raftGroups.length - 1);
    }

    public static void assertAllHasSameLeaderId(RaftGroup ... raftGroups) {
        Set<Integer> leaderIds = Arrays.asList(raftGroups).stream().map(RaftGroup::getCurrentLeaderId).collect(Collectors.toSet());
        assertThat(leaderIds).hasSize(1);
        assertThat(leaderIds.iterator().next()).isGreaterThan(0);
    }

    public static void assertVotedFor(RaftGroup ... raftGroups) {
        await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
            // TODO follower might increase term and vote for itself
            Set<Integer> votedForIds = Arrays.stream(raftGroups).map(raftGroup -> raftGroup.getRaftStorage().getVotedFor()).collect(Collectors.toSet());
            assertThat(votedForIds).hasSize(1);
            assertThat(votedForIds.iterator().next()).isGreaterThan(0);
        });
    }

    public static void assertTerm(RaftGroup ... raftGroups) {
        await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
            Set<Integer> terms = Arrays.asList(raftGroups).stream().map(raftGroup -> raftGroup.getRaftStorage().getTerm()).collect(Collectors.toSet());
            assertThat(terms).hasSize(1);
            assertThat(terms.iterator().next()).isGreaterThanOrEqualTo(1);
        });
    }

}
