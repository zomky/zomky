package rsocket.playground.raft;

import io.rsocket.util.ByteBufPayload;
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
import rsocket.playground.raft.storage.FileSystemZomkyStorage;
import rsocket.playground.raft.storage.FileSystemZomkyStorageTestUtils;
import rsocket.playground.raft.storage.LogEntryInfo;
import rsocket.playground.raft.storage.ZomkyStorage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.BDDMockito.given;

@RunWith(MockitoJUnitRunner.class)
public class NodeTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(NodeTest.class);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Mock
    ElectionTimeout electionTimeout1, electionTimeout2, electionTimeout3;

    Node node1, node2, node3;
    ZomkyStorage zomkyStorage1, zomkyStorage2, zomkyStorage3;

    @Before
    public void setUp() {
        LOGGER.info("Zomky directory {}", folder.getRoot().getAbsolutePath());
        zomkyStorage1 = new FileSystemZomkyStorage(7000, folder.getRoot().getAbsolutePath());
        zomkyStorage2 = new FileSystemZomkyStorage(7001, folder.getRoot().getAbsolutePath());
        zomkyStorage3 = new FileSystemZomkyStorage(7002, folder.getRoot().getAbsolutePath());

        StateMachine stateMachine = new ExampleStateMachine();

        node1 = Node.create(7000, zomkyStorage1, Arrays.asList(7001, 7002), stateMachine, electionTimeout1);
        node2 = Node.create(7001, zomkyStorage2, Arrays.asList(7000, 7002), stateMachine, electionTimeout2);
        node3 = Node.create(7002, zomkyStorage3, Arrays.asList(7000, 7001), stateMachine, electionTimeout3);
    }

    @Test
    public void testElection() {
        given(electionTimeout1.nextRandom()).willReturn(Duration.ofMillis(200));
        given(electionTimeout2.nextRandom()).willReturn(Duration.ofSeconds(10));
        given(electionTimeout3.nextRandom()).willReturn(Duration.ofSeconds(10));

        node1.start();
        node2.start();
        node3.start();

        await().atMost(1, TimeUnit.SECONDS).until(() -> node1.getCurrentLeaderId() == 7000);
        await().atMost(1, TimeUnit.SECONDS).until(() -> node2.getCurrentLeaderId() == 7000);
        await().atMost(1, TimeUnit.SECONDS).until(() -> node3.getCurrentLeaderId() == 7000);

        assertThat(node1.nodeState).isEqualTo(NodeState.LEADER);
        assertThat(node2.nodeState).isEqualTo(NodeState.FOLLOWER);
        assertThat(node3.nodeState).isEqualTo(NodeState.FOLLOWER);

        assertThat(zomkyStorage1.getTerm()).isEqualTo(1);
        assertThat(zomkyStorage1.getVotedFor()).isEqualTo(7000);
        assertThat(zomkyStorage2.getTerm()).isEqualTo(1);
        assertThat(zomkyStorage2.getVotedFor()).isEqualTo(7000);
        assertThat(zomkyStorage3.getTerm()).isEqualTo(1);
        assertThat(zomkyStorage3.getVotedFor()).isEqualTo(7000);
        assertThat(zomkyStorage1.getLast()).isEqualTo(new LogEntryInfo().index(0).term(0));
    }

    @Test
    public void testLogReplication() throws IOException {
        testElection();

        Client client = new Client(Arrays.asList(7000));
        client.start();

        int nbEntries = 10;

        client.send(Flux.range(1, nbEntries).map(i -> ByteBufPayload.create("Abc" + i)))
                .doOnSubscribe(subscription -> LOGGER.info("Client started"))
                .doOnNext(s -> LOGGER.info("Client received {}", s.getDataUtf8()))
                .doOnComplete(() -> LOGGER.info("Client finished"))
                .blockLast();

        await().atMost(1, TimeUnit.SECONDS).until(() -> zomkyStorage1.getLast().equals(new LogEntryInfo().index(nbEntries).term(1)));
        await().atMost(1, TimeUnit.SECONDS).until(() -> zomkyStorage2.getLast().equals(new LogEntryInfo().index(nbEntries).term(1)));
        await().atMost(1, TimeUnit.SECONDS).until(() -> zomkyStorage3.getLast().equals(new LogEntryInfo().index(nbEntries).term(1)));

        assertThat(FileSystemZomkyStorageTestUtils.getContent(folder.getRoot().getAbsolutePath(), 7000))
                .isEqualTo(expectedContent(nbEntries));

        assertThat(FileSystemZomkyStorageTestUtils.getContent(folder.getRoot().getAbsolutePath(), 7001))
                .isEqualTo(expectedContent(nbEntries));

        assertThat(FileSystemZomkyStorageTestUtils.getContent(folder.getRoot().getAbsolutePath(), 7002))
                .isEqualTo(expectedContent(nbEntries));
    }

    private String expectedContent(int nbEntries) {
        return IntStream.rangeClosed(1, nbEntries).mapToObj(i -> "Abc"+i).collect(Collectors.joining());
    }

    private static class ExampleStateMachine implements StateMachine {

        private static final Logger LOGGER = LoggerFactory.getLogger(ExampleStateMachine.class);

        @Override
        public ByteBuffer applyLogEntry(ByteBuffer entry) {
            String req = new String(entry.array());
            LOGGER.info("APPLY {}", req);
            return ByteBuffer.wrap((req + "-resp").getBytes());
        }

    }

}