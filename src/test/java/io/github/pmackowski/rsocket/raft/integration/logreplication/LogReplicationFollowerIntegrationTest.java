package io.github.pmackowski.rsocket.raft.integration.logreplication;

import io.github.pmackowski.rsocket.raft.*;
import io.github.pmackowski.rsocket.raft.integration.IntegrationTestsUtils;
import io.github.pmackowski.rsocket.raft.kvstore.KVStateMachine;
import io.github.pmackowski.rsocket.raft.rpc.AppendEntriesRequest;
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
import reactor.test.StepVerifier;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static io.github.pmackowski.rsocket.raft.integration.IntegrationTestsUtils.entry;
import static io.github.pmackowski.rsocket.raft.integration.IntegrationTestsUtils.logEntry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@IntegrationTest
class LogReplicationFollowerIntegrationTest {

    @TempDir
    Path directory;

    @Mock
    ElectionTimeout electionTimeout;

    Mono<RaftServer> raftServerMono;
    InternalRaftServer raftServer;
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
    void logReplication() {
        // given
        given(electionTimeout.nextRandom()).willReturn(Duration.ofMinutes(100));
        raftServerMono = monoFirstRaftServer();
        raftServer = (InternalRaftServer) raftServerMono.block();
        assertThat(raftServer.isFollower()).isTrue();

        // when
        AppendEntriesRequest appendEntriesRequest = AppendEntriesRequest.newBuilder()
                .setPrevLogIndex(0)
                .setPrevLogTerm(0)
                .setTerm(1)
                .setLeaderCommit(1)
                .setLeaderId(7000)
                .addEntries(entry(1, "key1", "val1"))
                .build();

        StepVerifier.create(raftServer.onAppendEntries(appendEntriesRequest))
                .assertNext(appendEntriesResponse -> {
                    assertThat(appendEntriesResponse.getTerm()).isEqualTo(0);
                    assertThat(appendEntriesResponse.getSuccess()).isEqualTo(true);
                    assertThat(raftStorage.getLastIndexedTerm().getIndex()).isEqualTo(1);
                    assertThat(logEntry(raftStorage, 1)).isEqualTo("key1=val1");
                    assertThat(raftServer.getCurrentLeaderId()).isEqualTo(7000);
                    assertThat(raftStorage.commitIndex()).isEqualTo(1);
                }).verifyComplete();
    }

    private Mono<RaftServer> monoFirstRaftServer() {
        return monoRaftServer(7000, raftStorage, electionTimeout);
    }

    private Mono<RaftServer> monoRaftServer(int nodeId, RaftStorage raftStorage, ElectionTimeout electionTimeout) {
        return new RaftServerBuilder()
                .nodeId(nodeId)
                .storage(raftStorage)
                .stateMachine(new KVStateMachine(nodeId))
                .electionTimeout(electionTimeout)
                .initialConfiguration(new Configuration(7000, 7001))
                .start();
    }

}
