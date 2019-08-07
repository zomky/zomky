package io.github.pmackowski.rsocket.raft.gossip;

import io.github.pmackowski.rsocket.raft.Node;
import io.github.pmackowski.rsocket.raft.NodeFactory;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Hooks;

class GossipProtocolTest {

    @Test
    void name() throws InterruptedException {

        Hooks.onOperatorDebug();

        NodeFactory.receive()
            .port(7000)
//            .retryJoin(7001)
            .start()
            .subscribe();

        Thread.sleep(5000);

        Node node2 = NodeFactory.receive()
                .port(7001)
//                .retryJoin(7000)
                .start()
                .block();

        node2.join(7000, false).subscribe();

        Thread.sleep(15000);

    }
}