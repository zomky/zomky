package rsocket.playground.raft;

import io.rsocket.util.DefaultPayload;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import rsocket.playground.raft.storage.FileSystemZomkyStorage;
import rsocket.playground.raft.storage.ZomkyStorage;

import java.util.Arrays;

public class NodeTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(NodeTest.class);

    @Test
    public void name() throws InterruptedException {

        Node node1 = Node.create(7000, new FileSystemZomkyStorage(7000), Arrays.asList(7001, 7002));
        Node node2 = Node.create(7001, new FileSystemZomkyStorage(7001), Arrays.asList(7000, 7002));
        Node node3 = Node.create(7002, new FileSystemZomkyStorage(7002), Arrays.asList(7000, 7001));

        node1.start();
        node2.start();
        node3.start();

        Thread.sleep(3000);

        Client client = new Client(Arrays.asList(7000,7001));
        client.start();

        client.send(Flux.range(1, 1000_000).map(i -> DefaultPayload.create("Abc"+i)))
              .doOnSubscribe(subscription -> LOGGER.info("Client started"))
              .doOnComplete(() -> LOGGER.info("Client finished"))
              .subscribe();
//              .subscribe(s -> LOGGER.info("Client received {}", s.getDataUtf8()));

        Thread.sleep(1000000);
    }

    @Test
    public void names() {
        ZomkyStorage zomkyStorage = new FileSystemZomkyStorage(7000);
        System.out.println("--> " + zomkyStorage.getTerm());
        System.out.println("--> " + zomkyStorage.getVotedFor());
        System.out.println("--> " + zomkyStorage.getLast().getTerm());
        System.out.println("--> " + zomkyStorage.getLast().getIndex());
        System.out.println("--> " + new String(zomkyStorage.getEntryByIndex(1).array()));
    }
}