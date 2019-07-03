package io.github.pmackowski.rsocket.raft.integration.clusterconfiguration;

import io.github.pmackowski.rsocket.raft.ElectionTimeout;
import io.github.pmackowski.rsocket.raft.IntegrationTest;
import io.github.pmackowski.rsocket.raft.RaftServer;
import io.github.pmackowski.rsocket.raft.RaftServerBuilder;
import io.github.pmackowski.rsocket.raft.kvstore.KVStateMachine;
import io.github.pmackowski.rsocket.raft.kvstore.KVStoreClient;
import io.github.pmackowski.rsocket.raft.kvstore.KeyValue;
import io.github.pmackowski.rsocket.raft.storage.FileSystemRaftStorage;
import io.github.pmackowski.rsocket.raft.storage.RaftStorage;
import io.github.pmackowski.rsocket.raft.storage.RaftStorageConfiguration;
import io.github.pmackowski.rsocket.raft.storage.log.SizeUnit;
import io.github.pmackowski.rsocket.raft.storage.meta.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
@IntegrationTest
class ClusterConfigurationIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterConfigurationIntegrationTest.class);

    @TempDir
    Path folder;

    @Mock
    ElectionTimeout electionTimeout;

    Mono<RaftServer> raftServerMono;
    RaftServer raftServer;
    RaftStorage raftStorage;

    @BeforeEach
    public void setUp() {
        BlockHound.builder()
                .allowBlockingCallsInside("java.io.FileInputStream", "readBytes")
                .install();

        raftStorage = raftStorage("1");

        raftServerMono = new RaftServerBuilder()
                .nodeId(7000)
                .storage(raftStorage)
                .stateMachine(new KVStateMachine(7000))
                .electionTimeout(electionTimeout)
                .initialConfiguration(new Configuration(7000))
                .start();
    }

    @AfterEach
    void tearDown() {
        raftStorage.close();
        raftServer.dispose();
    }

    @Test
    void testElection() {
        raftServer = raftServerMono.block();

        await().atMost(2, TimeUnit.SECONDS).until(() -> raftServer.getCurrentLeaderId() == 7000);

        assertThat(raftServer.isLeader()).isTrue();

        assertThat(raftStorage.getTerm()).isEqualTo(1);
        assertThat(raftStorage.getVotedFor()).isEqualTo(7000);
    }

    @Test
    void testLogReplication() {
        testElection();

        KVStoreClient kvStoreClient = new KVStoreClient(Arrays.asList(7000));
        kvStoreClient.start();

        int nbEntries = 10;

        kvStoreClient.put(Flux.range(1, nbEntries).delayElements(Duration.ofMillis(500)).map(i -> new KeyValue("key" + i, "val" + i)))
                .doOnSubscribe(subscription -> LOGGER.info("KVStoreClient started"))
                .doOnNext(s -> LOGGER.info("KVStoreClient received {}", s))
                .doOnComplete(() -> LOGGER.info("KVStoreClient finished"))
                .blockLast();

        await().atMost(1, TimeUnit.SECONDS).until(() -> raftStorage.getLastIndexedTerm().getIndex() == nbEntries);
    }

    private RaftStorage raftStorage(String node) {
        return new FileSystemRaftStorage(RaftStorageConfiguration.builder()
                .segmentSize(SizeUnit.megabytes, 1)
                .directory(Paths.get(folder.toAbsolutePath().toString(), "node" + node))
                .build()
        );
    }
}
