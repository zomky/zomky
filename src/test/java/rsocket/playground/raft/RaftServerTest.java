package rsocket.playground.raft;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rsocket.playground.raft.statemachine.kv.KVStateMachine;
import rsocket.playground.raft.statemachine.kv.KVStoreClient;
import rsocket.playground.raft.statemachine.kv.KeyValue;
import rsocket.playground.raft.storage.DefaultRaftStorage;
import rsocket.playground.raft.storage.DefaultRaftStorageTestUtils;
import rsocket.playground.raft.storage.LogEntryInfo;
import rsocket.playground.raft.storage.RaftStorage;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.BDDMockito.given;

@RunWith(MockitoJUnitRunner.class)
public class RaftServerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftServerTest.class);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Mock
    ElectionTimeout electionTimeout1, electionTimeout2, electionTimeout3;

    Mono<RaftServer> raftServerMono1, raftServerMono2, raftServerMono3;
    RaftServer raftServer1, raftServer2, raftServer3;
    RaftStorage raftStorage1, raftStorage2, raftStorage3;

    @Before
    public void setUp() {
        LOGGER.info("Raft directory {}", folder.getRoot().getAbsolutePath());
        raftStorage1 = new DefaultRaftStorage(7000, folder.getRoot().getAbsolutePath());
        raftStorage2 = new DefaultRaftStorage(7001, folder.getRoot().getAbsolutePath());
        raftStorage3 = new DefaultRaftStorage(7002, folder.getRoot().getAbsolutePath());

        raftServerMono1 = new RaftServerBuilder()
                    .nodeId(7000)
                    .storage(raftStorage1)
                    .clientPorts(Arrays.asList(7001, 7002))
                    .stateMachine(new KVStateMachine(7000))
                    .electionTimeout(electionTimeout1)
                    .start();
        raftServerMono2 = new RaftServerBuilder()
                    .nodeId(7001)
                    .storage(raftStorage2)
                    .clientPorts(Arrays.asList(7000, 7002))
                    .stateMachine(new KVStateMachine(7001))
                    .electionTimeout(electionTimeout2)
                    .start();
        raftServerMono3 = new RaftServerBuilder()
                    .nodeId(7002)
                    .storage(raftStorage3)
                    .clientPorts(Arrays.asList(7000, 7001))
                    .stateMachine(new KVStateMachine(7002))
                    .electionTimeout(electionTimeout3)
                    .start();
    }

    @Test
    public void testElection() {
        given(electionTimeout1.nextRandom()).willReturn(Duration.ofMillis(300));
        given(electionTimeout2.nextRandom()).willReturn(Duration.ofSeconds(10));
        given(electionTimeout3.nextRandom()).willReturn(Duration.ofSeconds(10));

        raftServer1 = raftServerMono1.block();
        raftServer2 = raftServerMono2.block();
        raftServer3 = raftServerMono3.block();

        await().atMost(1, TimeUnit.SECONDS).until(() -> raftServer1.getCurrentLeaderId() == 7000);
        await().atMost(1, TimeUnit.SECONDS).until(() -> raftServer2.getCurrentLeaderId() == 7000);
        await().atMost(1, TimeUnit.SECONDS).until(() -> raftServer3.getCurrentLeaderId() == 7000);

        assertThat(raftServer1.isLeader()).isTrue();
        assertThat(raftServer2.isFollower()).isTrue();
        assertThat(raftServer3.isFollower()).isTrue();

        assertThat(raftStorage1.getTerm()).isEqualTo(1);
        assertThat(raftStorage1.getVotedFor()).isEqualTo(7000);
        assertThat(raftStorage2.getTerm()).isEqualTo(1);
        assertThat(raftStorage2.getVotedFor()).isEqualTo(7000);
        assertThat(raftStorage3.getTerm()).isEqualTo(1);
        assertThat(raftStorage3.getVotedFor()).isEqualTo(7000);
        assertThat(raftStorage1.getLast()).isEqualTo(new LogEntryInfo().index(0).term(0));
    }

    @Test
    public void leaderFailure() {
        given(electionTimeout1.nextRandom()).willReturn(Duration.ofMillis(300));
        given(electionTimeout2.nextRandom()).willReturn(Duration.ofMillis(1000));
        given(electionTimeout3.nextRandom()).willReturn(Duration.ofSeconds(10));

        raftServer1 = raftServerMono1.block();
        raftServer2 = raftServerMono2.block();
        raftServer3 = raftServerMono3.block();

        await().atMost(1, TimeUnit.SECONDS).until(() -> raftServer1.getCurrentLeaderId() == 7000);
        await().atMost(1, TimeUnit.SECONDS).until(() -> raftServer2.getCurrentLeaderId() == 7000);
        await().atMost(1, TimeUnit.SECONDS).until(() -> raftServer3.getCurrentLeaderId() == 7000);

        assertThat(raftServer1.isLeader()).isTrue();
        assertThat(raftServer2.isFollower()).isTrue();
        assertThat(raftServer3.isFollower()).isTrue();

        assertThat(raftStorage1.getTerm()).isEqualTo(1);
        assertThat(raftStorage1.getVotedFor()).isEqualTo(7000);
        assertThat(raftStorage2.getTerm()).isEqualTo(1);
        assertThat(raftStorage2.getVotedFor()).isEqualTo(7000);
        assertThat(raftStorage3.getTerm()).isEqualTo(1);
        assertThat(raftStorage3.getVotedFor()).isEqualTo(7000);
        assertThat(raftStorage1.getLast()).isEqualTo(new LogEntryInfo().index(0).term(0));

        raftServer1.dispose();

        await().atMost(1, TimeUnit.SECONDS).until(() -> raftServer2.getCurrentLeaderId() == 7001);
        await().atMost(1, TimeUnit.SECONDS).until(() -> raftServer3.getCurrentLeaderId() == 7001);
        assertThat(raftServer2.isLeader()).isTrue();
        assertThat(raftServer3.isFollower()).isTrue();
        assertThat(raftStorage1.getTerm()).isEqualTo(1);
        assertThat(raftStorage1.getVotedFor()).isEqualTo(7000);
        assertThat(raftStorage2.getTerm()).isEqualTo(2);
        assertThat(raftStorage2.getVotedFor()).isEqualTo(7001);
        assertThat(raftStorage3.getTerm()).isEqualTo(2);
        assertThat(raftStorage3.getVotedFor()).isEqualTo(7001);
    }

    @Test
    public void testLogReplication() throws IOException {
        testElection();

        KVStoreClient kvStoreClient = new KVStoreClient(Arrays.asList(7000));
        kvStoreClient.start();

        int nbEntries = 10;

        kvStoreClient.put(Flux.range(1, nbEntries).delayElements(Duration.ofMillis(500)).map(i -> new KeyValue("key"+i, "val"+i)))
                .doOnSubscribe(subscription -> LOGGER.info("KVStoreClient started"))
                .doOnNext(s -> LOGGER.info("KVStoreClient received {}", s))
                .doOnComplete(() -> LOGGER.info("KVStoreClient finished"))
                .blockLast();

        await().atMost(1, TimeUnit.SECONDS).until(() -> raftStorage1.getLast().equals(new LogEntryInfo().index(nbEntries).term(1)));
        await().atMost(1, TimeUnit.SECONDS).until(() -> raftStorage2.getLast().equals(new LogEntryInfo().index(nbEntries).term(1)));
        await().atMost(1, TimeUnit.SECONDS).until(() -> raftStorage3.getLast().equals(new LogEntryInfo().index(nbEntries).term(1)));

        assertThat(DefaultRaftStorageTestUtils.getContent(folder.getRoot().getAbsolutePath(), 7000))
                .isEqualTo(expectedContent(nbEntries));

        assertThat(DefaultRaftStorageTestUtils.getContent(folder.getRoot().getAbsolutePath(), 7001))
                .isEqualTo(expectedContent(nbEntries));

        assertThat(DefaultRaftStorageTestUtils.getContent(folder.getRoot().getAbsolutePath(), 7002))
                .isEqualTo(expectedContent(nbEntries));
    }

    @Test
    public void testLogReplicationMultipleClients() {
        testElection();

        KVStoreClient kvStoreClient = new KVStoreClient(Arrays.asList(7000));
        kvStoreClient.start();

        int nbEntries = 10;

        kvStoreClient.put(Flux.range(1, nbEntries).delayElements(Duration.ofMillis(500)).map(i -> new KeyValue("key"+i, "val"+i)))
                .doOnSubscribe(subscription -> LOGGER.info("KVStoreClient started"))
                .doOnNext(s -> LOGGER.info("KVStoreClient received {}", s))
                .doOnComplete(() -> LOGGER.info("KVStoreClient finished"))
                .subscribe();

        kvStoreClient.put(Flux.range(11, nbEntries).delayElements(Duration.ofMillis(500)).map(i -> new KeyValue("key"+i, "val"+i)))
                .doOnSubscribe(subscription -> LOGGER.info("Client2 started"))
                .doOnNext(s -> LOGGER.info("Client2 received {}", s))
                .doOnComplete(() -> LOGGER.info("Client2 finished"))
                .subscribe();

        await().atMost(10, TimeUnit.SECONDS).until(() -> raftStorage1.getLast().equals(new LogEntryInfo().index(nbEntries * 2).term(1)));
        await().atMost(10, TimeUnit.SECONDS).until(() -> raftStorage2.getLast().equals(new LogEntryInfo().index(nbEntries * 2).term(1)));
        await().atMost(10, TimeUnit.SECONDS).until(() -> raftStorage3.getLast().equals(new LogEntryInfo().index(nbEntries * 2).term(1)));
    }

    @Test
    public void testLogReplicationWithLeaderFailure() throws InterruptedException {
        testElection();

        KVStoreClient kvStore = new KVStoreClient(Arrays.asList(7000));
        kvStore.start();

        int nbEntries = 10;

        kvStore.put(Flux.range(1, nbEntries).delayElements(Duration.ofMillis(100)).map(i -> new KeyValue("key"+i, "val"+i)))
                .doOnSubscribe(subscription -> LOGGER.info("Client1 started"))
                .doOnNext(s -> LOGGER.info("Client1 received {}", s))
                .doOnComplete(() -> LOGGER.info("Client1 finished"))
                .blockLast();

        Thread.sleep(3000);

        raftServer1.dispose();
        raftServer2.dispose();
        raftServer3.dispose();

        Thread.sleep(2000);

        given(electionTimeout1.nextRandom()).willReturn(Duration.ofSeconds(10));
        given(electionTimeout2.nextRandom()).willReturn(Duration.ofMillis(300));
        given(electionTimeout3.nextRandom()).willReturn(Duration.ofSeconds(1));

        raftServer1 = raftServerMono1.block();
        raftServer2 = raftServerMono2.block();
        raftServer3 = raftServerMono3.block();

        KVStoreClient kvStore2 = new KVStoreClient(Arrays.asList(7001));
        kvStore2.start();

        Thread.sleep(2000);
        kvStore2.put(Flux.range(11, nbEntries).map(i -> new KeyValue("key"+i, "val"+i)))
                .doOnSubscribe(subscription -> LOGGER.info("Client2 started"))
                .doOnNext(s -> LOGGER.info("Client2 received {}", s))
                .doOnComplete(() -> LOGGER.info("Client2 finished"))
                .blockLast();

        await().atMost(10, TimeUnit.SECONDS).until(() -> raftStorage1.getLast().equals(new LogEntryInfo().index(nbEntries * 2).term(2)));
        await().atMost(10, TimeUnit.SECONDS).until(() -> raftStorage2.getLast().equals(new LogEntryInfo().index(nbEntries * 2).term(2)));
        await().atMost(10, TimeUnit.SECONDS).until(() -> raftStorage3.getLast().equals(new LogEntryInfo().index(nbEntries * 2).term(2)));
    }

    private String expectedContent(int nbEntries) {
        return IntStream.rangeClosed(1, nbEntries).mapToObj(i -> "Abc"+i).collect(Collectors.joining());
    }

}