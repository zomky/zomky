package rsocket.playground.raft;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rsocket.playground.raft.h2.H2RepositoryImpl;

import java.util.Arrays;

public class NodeTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(NodeTest.class);

    @Test
    public void name() throws InterruptedException {
        NodeRepository nodeRepository = new H2RepositoryImpl();

        Node node1 = Node.create(7000, nodeRepository, Arrays.asList(7001, 7002));
        Node node2 = Node.create(7001, nodeRepository, Arrays.asList(7000, 7002));
        Node node3 = Node.create(7002, nodeRepository, Arrays.asList(7000, 7001));

        node1.start();
        node2.start();
        node3.start();

        Thread.sleep(5000);
        node1.stop();

        Thread.sleep(1000000);
    }
}