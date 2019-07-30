package io.github.pmackowski.rsocket.raft.integration.gossip;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GossipTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(GossipTest.class);

    @Test
    public void echoTest() throws Exception {
        GossipNode node1 = new GossipNode(7000);
        GossipNode node2 = new GossipNode(7001);
        GossipNode node3 = new GossipNode(7002);
        GossipNode node4 = new GossipNode(7003);

        Thread.sleep(2_000);

        node1.ping(7001, 7002, 7003).subscribe();
        node2.ping(7002, 7001, 7003).subscribe();

        Thread.sleep(100);

        node3.ping(7001, 7002, 7000).subscribe();
        node4.ping(7002, 7001, 7003).subscribe();


//        node3.ping(7000, 7001, 7003).subscribe();
//        node1.ping(7001).block();

        Thread.sleep(15_000);

//        node1.disposeNow();
//        node2.disposeNow();
//        node3.disposeNow();
    }

}
