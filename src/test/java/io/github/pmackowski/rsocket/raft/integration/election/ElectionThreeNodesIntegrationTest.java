package io.github.pmackowski.rsocket.raft.integration.election;

import io.github.pmackowski.rsocket.raft.ElectionTimeout;
import io.github.pmackowski.rsocket.raft.IntegrationTest;
import io.github.pmackowski.rsocket.raft.RaftServer;
import io.github.pmackowski.rsocket.raft.RaftServerBuilder;
import io.github.pmackowski.rsocket.raft.integration.IntegrationTestsUtils;
import io.github.pmackowski.rsocket.raft.kvstore.KVStateMachine;
import io.github.pmackowski.rsocket.raft.storage.RaftStorage;
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
class ElectionThreeNodesIntegrationTest {

    @TempDir
    Path directory;

    @Mock
    ElectionTimeout electionTimeout1, electionTimeout2, electionTimeout3;

    Mono<RaftServer> raftServerMono1, raftServerMono2, raftServerMono3;
    RaftServer raftServer1, raftServer2, raftServer3;
    RaftStorage raftStorage1, raftStorage2, raftStorage3;

    @BeforeEach
    void setUp() {
        IntegrationTestsUtils.checkBlockingCalls();
        raftStorage1 = IntegrationTestsUtils.raftStorage(directory, "1");
        raftStorage2 = IntegrationTestsUtils.raftStorage(directory, "2");
        raftStorage3 = IntegrationTestsUtils.raftStorage(directory, "3");
    }

    @AfterEach
    void tearDown() {
        raftStorage1.close();
        raftStorage2.close();
        raftStorage3.close();
        raftServer1.dispose();
        raftServer2.dispose();
        if (raftServer3 != null) {
            raftServer3.dispose();
        }
    }

    @Test
    void electionPreVoteDisabled() {
        // given
        given(electionTimeout1.nextRandom()).willReturn(Duration.ofMillis(50));
        given(electionTimeout2.nextRandom()).willReturn(Duration.ofSeconds(10));
        given(electionTimeout3.nextRandom()).willReturn(Duration.ofSeconds(10));

        raftServerMono1 = monoFirstRaftServer(false);
        raftServerMono2 = monoSecondRaftServer(false);
        raftServerMono3 = monoThirdRaftServer(false);

        // when
        raftServer1 = raftServerMono1.block();
        raftServer2 = raftServerMono2.block();
        raftServer3 = raftServerMono3.block();

        // then
        await().atMost(2, TimeUnit.SECONDS).until(() ->
                raftServer1.isLeader() &&
                raftServer2.isFollower() &&
                raftServer3.isFollower()
        );
        await().atMost(1, TimeUnit.SECONDS).until(() ->
                raftServer1.getCurrentLeaderId() == 7000 &&
                raftServer2.getCurrentLeaderId() == 7000 &&
                raftServer3.getCurrentLeaderId() == 7000
        );

        assertThat(raftStorage1.getTerm()).isGreaterThanOrEqualTo(1);
        assertThat(raftStorage2.getTerm()).isGreaterThanOrEqualTo(1);
        assertThat(raftStorage3.getTerm()).isGreaterThanOrEqualTo(1);
        assertThat(raftStorage1.getTerm()).isEqualTo(raftStorage2.getTerm());
        assertThat(raftStorage2.getTerm()).isEqualTo(raftStorage3.getTerm());

        assertThat(raftStorage1.getVotedFor()).isEqualTo(7000);
        assertThat(raftStorage2.getVotedFor() == 7000 || raftStorage3.getVotedFor() == 7000).isTrue();
    }

    @Test
    void electionPreVoteDisabledOneNodeUnavailable() {
        // given
        given(electionTimeout1.nextRandom()).willReturn(Duration.ofMillis(50));
        given(electionTimeout2.nextRandom()).willReturn(Duration.ofSeconds(10));

        raftServerMono1 = monoFirstRaftServer(false);
        raftServerMono2 = monoSecondRaftServer(false);

        // when
        raftServer1 = raftServerMono1.block();
        raftServer2 = raftServerMono2.block();

        // then
        await().atMost(2, TimeUnit.SECONDS).until(() ->
                raftServer1.isLeader() &&
                raftServer2.isFollower()
        );
        await().atMost(1, TimeUnit.SECONDS).until(() ->
                raftServer1.getCurrentLeaderId() == 7000 &&
                raftServer2.getCurrentLeaderId() == 7000
        );

        assertThat(raftStorage1.getTerm()).isGreaterThanOrEqualTo(1);
        assertThat(raftStorage2.getTerm()).isGreaterThanOrEqualTo(1);
        assertThat(raftStorage3.getTerm()).isEqualTo(0);
        assertThat(raftStorage1.getTerm()).isEqualTo(raftStorage2.getTerm());

        assertThat(raftStorage1.getVotedFor()).isEqualTo(7000);
        assertThat(raftStorage2.getVotedFor()).isEqualTo(7000);
    }

    @Test
    void electionPreVoteEnabled() {
        // given
        given(electionTimeout1.nextRandom()).willReturn(Duration.ofMillis(50));
        given(electionTimeout2.nextRandom()).willReturn(Duration.ofSeconds(10));
        given(electionTimeout3.nextRandom()).willReturn(Duration.ofSeconds(10));

        raftServerMono1 = monoFirstRaftServer(true);
        raftServerMono2 = monoSecondRaftServer(true);
        raftServerMono3 = monoThirdRaftServer(true);

        // when
        raftServer1 = raftServerMono1.block();
        raftServer2 = raftServerMono2.block();
        raftServer3 = raftServerMono3.block();

        // then
        await().atMost(2, TimeUnit.SECONDS).until(() ->
                raftServer1.isLeader() &&
                raftServer2.isFollower() &&
                raftServer3.isFollower()
        );
        await().atMost(1, TimeUnit.SECONDS).until(() ->
                raftServer1.getCurrentLeaderId() == 7000 &&
                raftServer2.getCurrentLeaderId() == 7000 &&
                raftServer3.getCurrentLeaderId() == 7000
        );

        assertThat(raftStorage1.getTerm()).isEqualTo(1);
        assertThat(raftStorage2.getTerm()).isEqualTo(1);
        assertThat(raftStorage3.getTerm()).isEqualTo(1);

        assertThat(raftStorage1.getVotedFor()).isEqualTo(7000);
        assertThat(raftStorage2.getVotedFor() == 7000 || raftStorage3.getVotedFor() == 7000).isTrue();
    }


    private Mono<RaftServer> monoFirstRaftServer(boolean preVote) {
        return monoRaftServer(7000, raftStorage1, electionTimeout1, preVote);
    }

    private Mono<RaftServer> monoSecondRaftServer(boolean preVote) {
        return monoRaftServer(7001, raftStorage2, electionTimeout2, preVote);
    }

    private Mono<RaftServer> monoThirdRaftServer(boolean preVote) {
        return monoRaftServer(7002, raftStorage3, electionTimeout3, preVote);
    }

    private Mono<RaftServer> monoRaftServer(int nodeId, RaftStorage raftStorage, ElectionTimeout electionTimeout, boolean preVote) {
        return new RaftServerBuilder()
                .nodeId(nodeId)
                .storage(raftStorage)
                .stateMachine(new KVStateMachine(nodeId))
                .electionTimeout(electionTimeout)
                .initialConfiguration(new Configuration(7000, 7001, 7002))
                .preVote(preVote)
                .start();
    }

}
