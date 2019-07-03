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
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@ExtendWith(MockitoExtension.class)
@IntegrationTest
class ElectionOneNodeIntegrationTest {

    @TempDir
    Path directory;

    @Mock
    ElectionTimeout electionTimeout;

    Mono<RaftServer> raftServerMono;
    RaftServer raftServer;
    RaftStorage raftStorage;

    @BeforeEach
    void setUp() {
        IntegrationTestsUtils.checkBlockingCalls();
        raftStorage = IntegrationTestsUtils.raftStorage(directory);
    }

    @AfterEach
    void tearDown() {
        raftStorage.close();
        raftServer.dispose();
    }

    @Test
    void electionPreVoteDisabled() {
        // given
        raftServerMono = monoFirstRaftServer(false);

        // when
        raftServer = raftServerMono.block();

        // then
        await().atMost(1, TimeUnit.SECONDS).until(() -> raftServer.getCurrentLeaderId() == 7000);

        assertThat(raftServer.isLeader()).isTrue();
        assertThat(raftServer.getElectionTimeout()).isEqualTo(electionTimeout);
        assertThat(raftStorage.getTerm()).isEqualTo(1);
        assertThat(raftStorage.getVotedFor()).isEqualTo(7000);
    }

    @Test
    void electionPreVoteEnabled() {
        // given
        raftServerMono = monoFirstRaftServer(true);

        // when
        raftServer = raftServerMono.block();

        // then
        await().atMost(1, TimeUnit.SECONDS).until(() -> raftServer.getCurrentLeaderId() == 7000);

        assertThat(raftServer.isLeader()).isTrue();
        assertThat(raftServer.getElectionTimeout()).isEqualTo(electionTimeout);
        assertThat(raftStorage.getTerm()).isEqualTo(1);
        assertThat(raftStorage.getVotedFor()).isEqualTo(7000);
    }

    private Mono<RaftServer> monoFirstRaftServer(boolean preVote) {
        return monoRaftServer(7000, raftStorage, electionTimeout, preVote);
    }

    private Mono<RaftServer> monoRaftServer(int nodeId, RaftStorage raftStorage, ElectionTimeout electionTimeout, boolean preVote) {
        return new RaftServerBuilder()
                .nodeId(nodeId)
                .storage(raftStorage)
                .stateMachine(new KVStateMachine(nodeId))
                .electionTimeout(electionTimeout)
                .initialConfiguration(new Configuration(7000))
                .preVote(preVote)
                .start();
    }

}
