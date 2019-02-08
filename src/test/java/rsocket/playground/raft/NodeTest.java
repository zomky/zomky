package rsocket.playground.raft;

import io.rsocket.util.DefaultPayload;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rsocket.playground.raft.storage.FileSystemZomkyNodeStorage;
import rsocket.playground.raft.storage.ZomkyNodeStorage;

import java.util.Arrays;

public class NodeTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(NodeTest.class);

    @Test
    public void name() throws InterruptedException {
        ZomkyNodeStorage zomkyNodeStorage = new FileSystemZomkyNodeStorage();

        Node node1 = Node.create(7000, zomkyNodeStorage, Arrays.asList(7001, 7002));
        Node node2 = Node.create(7001, zomkyNodeStorage, Arrays.asList(7000, 7002));
        Node node3 = Node.create(7002, zomkyNodeStorage, Arrays.asList(7000, 7001));

        node1.start();
        node2.start();
        node3.start();

        Thread.sleep(6000);

        Client client = new Client(Arrays.asList(7000,7001));
        client.start();

        client.send(DefaultPayload.create("Abc"))
              .subscribe(s -> LOGGER.info("Client received {}", s.getDataUtf8()));

        Thread.sleep(1000000);
    }

}