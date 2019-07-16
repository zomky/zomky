package io.github.pmackowski.rsocket.raft.integration.election;

import io.github.pmackowski.rsocket.raft.IntegrationTest;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@ExtendWith(MockitoExtension.class)
@IntegrationTest
class ElectionOneNodeIntegrationTest {
/*
    @TempDir
    Path directory;

    @Mock
    ElectionTimeout electionTimeout;

    Mono<Node> raftServerMono;
    Node raftServer;
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

    private Mono<Node> monoFirstRaftServer(boolean preVote) {
        return monoRaftServer(7000, raftStorage, electionTimeout, preVote);
    }

    private Mono<Node> monoRaftServer(int nodeId, RaftStorage raftStorage, ElectionTimeout electionTimeout, boolean preVote) {
        return new NodeFactory()
                .nodeId(nodeId)
                .storage(raftStorage)
                .stateMachine(new KVStateMachine(nodeId))
                .stateMachineEntryConverter(new KVStateMachineEntryConverter())
                .electionTimeout(electionTimeout)
                .initialConfiguration(new Configuration(7000))
                .preVote(preVote)
                .start();
    }
*/
}
