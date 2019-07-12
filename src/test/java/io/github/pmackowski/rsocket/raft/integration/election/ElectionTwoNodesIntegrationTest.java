package io.github.pmackowski.rsocket.raft.integration.election;

import io.github.pmackowski.rsocket.raft.ElectionTimeout;
import io.github.pmackowski.rsocket.raft.IntegrationTest;
import io.github.pmackowski.rsocket.raft.RaftServer;
import io.github.pmackowski.rsocket.raft.RaftServerBuilder;
import io.github.pmackowski.rsocket.raft.external.statemachine.KVStateMachineEntryConverter;
import io.github.pmackowski.rsocket.raft.integration.IntegrationTestsUtils;
import io.github.pmackowski.rsocket.raft.kvstore.KVStateMachine;
import io.github.pmackowski.rsocket.raft.storage.RaftStorage;
import io.github.pmackowski.rsocket.raft.storage.log.entry.CommandEntry;
import io.github.pmackowski.rsocket.raft.storage.meta.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
@IntegrationTest
class ElectionTwoNodesIntegrationTest {

    @TempDir
    Path directory;

    @Mock
    ElectionTimeout electionTimeout1, electionTimeout2;

    Mono<RaftServer> raftServerMono1, raftServerMono2;
    RaftServer raftServer1, raftServer2;
    RaftStorage raftStorage1, raftStorage2;

    @BeforeEach
    void setUp() {
        IntegrationTestsUtils.checkBlockingCalls();
        raftStorage1 = IntegrationTestsUtils.raftStorage(directory, "1");
        raftStorage2 = IntegrationTestsUtils.raftStorage(directory, "2");
    }

    @AfterEach
    void tearDown() {
        raftStorage1.close();
        raftStorage2.close();
        raftServer1.dispose();
        if (raftServer2 != null) {
            raftServer2.dispose();
        }
    }

    @Test
    void electionPreVoteDisabled() {
        // given
        given(electionTimeout1.nextRandom()).willReturn(Duration.ofMillis(50));
        given(electionTimeout2.nextRandom()).willReturn(Duration.ofSeconds(10));

        raftServerMono1 = monoFirstRaftServer(false);
        raftServerMono2 = monoSecondRaftServer(false);

        // when
        raftServer1 = raftServerMono1.block();
        raftServer2 = raftServerMono2.block();

        // then
        await().atMost(1, TimeUnit.SECONDS).until(() -> raftServer1.isLeader());
        assertThat(raftServer2.isFollower()).isTrue();

        assertThat(raftServer1.getCurrentLeaderId()).isEqualTo(7000);
        assertThat(raftServer2.getCurrentLeaderId()).isEqualTo(7000);

        assertThat(raftStorage1.getTerm()).isGreaterThanOrEqualTo(1);
        assertThat(raftStorage2.getTerm()).isGreaterThanOrEqualTo(1);
        assertThat(raftStorage1.getTerm()).isEqualTo(raftStorage2.getTerm());

        assertThat(raftStorage1.getVotedFor()).isEqualTo(7000);
        assertThat(raftStorage2.getVotedFor()).isEqualTo(7000);
    }

    @Test
    void electionPreVoteDisabledOneNodeUnavailable() throws InterruptedException {
        // given
        given(electionTimeout1.nextRandom()).willReturn(Duration.ofMillis(50));

        raftServerMono1 = monoFirstRaftServer(false);

        // when
        raftServer1 = raftServerMono1.block();
        Thread.sleep(1000);

        // then
        assertThat(raftServer1.isCandidate()).isTrue();
        assertThat(raftStorage1.getVotedFor()).isEqualTo(7000);
        assertThat(raftStorage1.getTerm()).isGreaterThanOrEqualTo(10); // term inflation
    }

    @Test
    void electionPreVoteDisabledLowerTerm() {
        // given
        given(electionTimeout1.nextRandom()).willReturn(Duration.ofMillis(300));
        given(electionTimeout2.nextRandom()).willReturn(Duration.ofSeconds(10));

        raftStorage2.update(1000, 7001);

        raftServerMono1 = monoFirstRaftServer(false);
        raftServerMono2 = monoSecondRaftServer(false);

        // when
        raftServer1 = raftServerMono1.block();
        raftServer2 = raftServerMono2.block();

        // then
        await().atMost(2, TimeUnit.SECONDS).until(() -> raftServer1.isLeader());
        await().atMost(1, TimeUnit.SECONDS).until(() -> raftServer2.isFollower());

        assertThat(raftServer1.getCurrentLeaderId()).isEqualTo(7000);
        assertThat(raftServer2.getCurrentLeaderId()).isEqualTo(7000);

        assertThat(raftStorage1.getTerm()).isGreaterThanOrEqualTo(1001);
        assertThat(raftStorage2.getTerm()).isGreaterThanOrEqualTo(1001);
        assertThat(raftStorage1.getTerm()).isEqualTo(raftStorage2.getTerm());

        assertThat(raftStorage1.getVotedFor()).isEqualTo(7000);
        assertThat(raftStorage2.getVotedFor()).isEqualTo(7000);
    }

    @Test
    void electionPreVoteDisabledLogNotUpToDate() {
        // given
        given(electionTimeout1.nextRandom()).willReturn(Duration.ofMillis(300));
        given(electionTimeout2.nextRandom()).willReturn(Duration.ofSeconds(1));

        raftStorage2.update(1000, 7001);
        raftStorage2.append(new CommandEntry(1000, System.currentTimeMillis(), "abc".getBytes()));

        raftServerMono1 = monoFirstRaftServer(false);
        raftServerMono2 = monoSecondRaftServer(false);

        // when
        raftServer1 = raftServerMono1.block();
        raftServer2 = raftServerMono2.block();

        // then
        await().atMost(1, TimeUnit.SECONDS).until(() -> raftServer1.isFollower());
        await().atMost(2, TimeUnit.SECONDS).until(() -> raftServer2.isLeader());

        await().atMost(1, TimeUnit.SECONDS).until(() -> raftServer1.getCurrentLeaderId() == 7001);
        await().atMost(1, TimeUnit.SECONDS).until(() -> raftServer2.getCurrentLeaderId() == 7001);

        assertThat(raftStorage1.getTerm()).isGreaterThanOrEqualTo(1001);
        assertThat(raftStorage2.getTerm()).isGreaterThanOrEqualTo(1001);
        assertThat(raftStorage1.getTerm()).isEqualTo(raftStorage2.getTerm());

        assertThat(raftStorage1.getVotedFor()).isEqualTo(7001);
        assertThat(raftStorage2.getVotedFor()).isEqualTo(7001);
    }

    @Test
    void electionPreVoteEnabled() {
        // given
        given(electionTimeout1.nextRandom()).willReturn(Duration.ofMillis(50));
        given(electionTimeout2.nextRandom()).willReturn(Duration.ofSeconds(10));

        raftServerMono1 = monoFirstRaftServer(true);
        raftServerMono2 = monoSecondRaftServer(true);

        // when
        raftServer1 = raftServerMono1.block();
        raftServer2 = raftServerMono2.block();

        // then
        await().atMost(1, TimeUnit.SECONDS).until(() -> raftServer1.getCurrentLeaderId() == 7000);
        await().atMost(1, TimeUnit.SECONDS).until(() -> raftServer2.getCurrentLeaderId() == 7000);

        assertThat(raftServer1.isLeader()).isTrue();
        assertThat(raftServer2.isFollower()).isTrue();

        assertThat(raftStorage1.getTerm()).isGreaterThanOrEqualTo(1);
        assertThat(raftStorage2.getTerm()).isGreaterThanOrEqualTo(1);
        assertThat(raftStorage1.getTerm()).isEqualTo(raftStorage2.getTerm());

        assertThat(raftStorage1.getVotedFor()).isEqualTo(7000);
        assertThat(raftStorage2.getVotedFor()).isEqualTo(7000);
    }

    @Test
    void electionPreVoteEnabledOneNodeUnavailable() throws InterruptedException {
        // given
        given(electionTimeout1.nextRandom()).willReturn(Duration.ofMillis(50));

        raftServerMono1 = monoFirstRaftServer(true);

        // when
        raftServer1 = raftServerMono1.block();
        Thread.sleep(1000);

        // then
        assertThat(raftServer1.isFollower()).isTrue();
        assertThat(raftStorage1.getVotedFor()).isEqualTo(0);
        assertThat(raftStorage1.getTerm()).isEqualTo(0); // no term inflation
    }

    @Test
    void electionPreVoteEnabledLogNotUpToDate() {
        // given
        given(electionTimeout1.nextRandom()).willReturn(Duration.ofMillis(300));
        given(electionTimeout2.nextRandom()).willReturn(Duration.ofSeconds(1));

        raftStorage2.update(1000, 7001);
        raftStorage2.append(new CommandEntry(1000, System.currentTimeMillis(), "abc".getBytes()));

        raftServerMono1 = monoFirstRaftServer(true);
        raftServerMono2 = monoSecondRaftServer(true);

        // when
        raftServer1 = raftServerMono1.block();
        raftServer2 = raftServerMono2.block();

        // then
        await().atMost(2, TimeUnit.SECONDS).until(() -> raftServer2.isLeader());
        assertThat(raftServer1.isFollower()).isTrue();

        assertThat(raftServer1.getCurrentLeaderId()).isEqualTo(7001);
        assertThat(raftServer2.getCurrentLeaderId()).isEqualTo(7001);

        assertThat(raftStorage1.getTerm()).isGreaterThanOrEqualTo(1001);
        assertThat(raftStorage2.getTerm()).isGreaterThanOrEqualTo(1001);
        assertThat(raftStorage1.getTerm()).isEqualTo(raftStorage2.getTerm());

        assertThat(raftStorage1.getVotedFor()).isEqualTo(7001);
        assertThat(raftStorage2.getVotedFor()).isEqualTo(7001);
    }


    private Mono<RaftServer> monoFirstRaftServer(boolean preVote) {
        return monoRaftServer(7000, raftStorage1, electionTimeout1, preVote);
    }

    private Mono<RaftServer> monoSecondRaftServer(boolean preVote) {
        return monoRaftServer(7001, raftStorage2, electionTimeout2, preVote);
    }

    private Mono<RaftServer> monoRaftServer(int nodeId, RaftStorage raftStorage, ElectionTimeout electionTimeout, boolean preVote) {
        return new RaftServerBuilder()
                .nodeId(nodeId)
                .storage(raftStorage)
                .stateMachine(new KVStateMachine(nodeId))
                .stateMachineEntryConverter(new KVStateMachineEntryConverter())
                .electionTimeout(electionTimeout)
                .initialConfiguration(new Configuration(7000, 7001))
                .preVote(preVote)
                .start();
    }

}