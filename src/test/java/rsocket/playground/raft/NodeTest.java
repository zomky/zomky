package rsocket.playground.raft;

import io.rsocket.util.DefaultPayload;
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

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

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

    @Test
    public void testElection() {
        given(electionTimeout1.nextRandom()).willReturn(Duration.ofMillis(100));
        given(electionTimeout2.nextRandom()).willReturn(Duration.ofSeconds(10));
        given(electionTimeout3.nextRandom()).willReturn(Duration.ofSeconds(10));

        System.out.println(folder.getRoot().getAbsolutePath());

        node1 = Node.create(7000, new FileSystemZomkyStorage(7000, folder.getRoot().getAbsolutePath()),
                Arrays.asList(7001, 7002), electionTimeout1);
        node2 = Node.create(7001, new FileSystemZomkyStorage(7001, folder.getRoot().getAbsolutePath()),
                Arrays.asList(7000, 7002), electionTimeout2);
        node3 = Node.create(7002, new FileSystemZomkyStorage(7002, folder.getRoot().getAbsolutePath()),
                Arrays.asList(7000, 7001), electionTimeout3);

        node1.start();
        node2.start();
        node3.start();

        await().atMost(1, TimeUnit.SECONDS).until(() -> node1.getCurrentLeaderId() == 7000);
        await().atMost(1, TimeUnit.SECONDS).until(() -> node2.getCurrentLeaderId() == 7000);
        await().atMost(1, TimeUnit.SECONDS).until(() -> node3.getCurrentLeaderId() == 7000);

        assertThat(node1.nodeState).isEqualTo(NodeState.LEADER);
        assertThat(node2.nodeState).isEqualTo(NodeState.FOLLOWER);
        assertThat(node3.nodeState).isEqualTo(NodeState.FOLLOWER);
    }

    @Test
    public void testLogReplication() {
        testElection();

        Client client = new Client(Arrays.asList(7000));
        client.start();

        client.send(Flux.range(1, 10).delayElements(Duration.ofMillis(50)).map(i -> DefaultPayload.create("Abc"+i)))
                .doOnSubscribe(subscription -> LOGGER.info("Client started"))
                .doOnComplete(() -> LOGGER.info("Client finished"))
                .blockLast();
        // TODO
    }

}