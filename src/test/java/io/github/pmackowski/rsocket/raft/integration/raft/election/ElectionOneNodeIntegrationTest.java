package io.github.pmackowski.rsocket.raft.integration.raft.election;

import io.github.pmackowski.rsocket.raft.IntegrationTest;
import io.github.pmackowski.rsocket.raft.Node;
import io.github.pmackowski.rsocket.raft.Nodes;
import io.github.pmackowski.rsocket.raft.integration.IntegrationTestsUtils;
import io.github.pmackowski.rsocket.raft.raft.ElectionTimeout;
import io.github.pmackowski.rsocket.raft.raft.RaftGroup;
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
    }

    @Test
    void electionPreVoteDisabled() {
        // given
        Nodes nodes = Nodes.create(7000);

        // when
        nodes.addRaftGroup("group1", new Configuration(7000), builder -> builder.preVote(false));

        // then
        RaftGroup actual = nodes.raftGroup(7000, "group1");
        RaftStorage raftStorage = actual.getRaftStorage();

        await().atMost(1, TimeUnit.SECONDS).until(() -> actual.getCurrentLeaderId() == 7000);

        assertThat(actual.isLeader()).isTrue();
        assertThat(raftStorage.getTerm()).isEqualTo(1);
        assertThat(raftStorage.getVotedFor()).isEqualTo(7000);

        nodes.dispose();
    }

    @Test
    void electionPreVoteEnabled() {
        // given
        Nodes nodes = Nodes.create(7000);

        // when
        nodes.addRaftGroup("group1", new Configuration(7000), builder -> builder.preVote(true));

        // then
        RaftGroup actual = nodes.raftGroup(7000, "group1");
        RaftStorage raftStorage = actual.getRaftStorage();

        await().atMost(1, TimeUnit.SECONDS).until(() -> actual.getCurrentLeaderId() == 7000);

        assertThat(actual.isLeader()).isTrue();
        assertThat(raftStorage.getTerm()).isEqualTo(1);
        assertThat(raftStorage.getVotedFor()).isEqualTo(7000);

        nodes.dispose();
    }

}
