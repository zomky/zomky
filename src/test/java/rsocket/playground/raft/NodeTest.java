package rsocket.playground.raft;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class NodeTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(NodeTest.class);

    @Test
    public void name() throws InterruptedException {
        Node node1 = new Node(7000, Arrays.asList(7001, 7002));
        Node node2 = new Node(7001, Arrays.asList(7000, 7002));
        Node node3 = new Node(7002, Arrays.asList(7000, 7001));

        node1.start();
        //Thread.sleep(2000);
        node2.start();
        node3.start();

        Thread.sleep(1000000);
    }
}