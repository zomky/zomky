package io.github.zomky.docs;

import io.github.zomky.Node;
import io.github.zomky.NodeFactory;

public class ZomkyDocumentation {

    public void overview() {
        // tag::overview[]
        Node bob = NodeFactory.receive()    // <1>
                .nodeName("bob")
                .port(7000)
                .start()
                .block();

        Node alice = NodeFactory.receive()  // <2>
                .nodeName("alice")
                .port(7000)
                .retryJoin(7001)
                .start()
                .block();
        // end::overview[]
    }
}
