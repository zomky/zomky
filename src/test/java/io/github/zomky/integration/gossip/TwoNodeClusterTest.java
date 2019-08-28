package io.github.zomky.integration.gossip;

import io.github.zomky.Node;
import io.github.zomky.integration.TestNodeFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Disabled
public class TwoNodeClusterTest {

    Node node1, node2;
    Disposable node1Disposable;

    @AfterEach
    void tearDown() {
        if (node1 != null) {
            node1.dispose();
        }
        if (node2 != null) {
            node2.dispose();
        }
        if (node1Disposable != null) {
            node1Disposable.dispose();
        }
    }

    @Test
    void join() {
        node1 = TestNodeFactory.receive()
                .port(7000)
                .baseProbeTimeout(Duration.ofMillis(100))
                .baseProbeInterval(Duration.ofMillis(50))
                .start()
                .block();

        node2 = TestNodeFactory.receive()
                .port(7001)
                .baseProbeTimeout(Duration.ofMillis(100))
                .baseProbeInterval(Duration.ofMillis(50))
                .start()
                .block();

        node1.join(7001, false).subscribe();


        await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(node1.getCluster().getMembers()).containsExactlyInAnyOrder(7000, 7001);
            assertThat(node2.getCluster().getMembers()).containsExactlyInAnyOrder(7000, 7001);
        });
    }

    @Test
    void retryJoin() {
        node1Disposable = TestNodeFactory.receive()
                .port(7000)
                .retryJoin(7001)
                .baseProbeTimeout(Duration.ofMillis(100))
                .baseProbeInterval(Duration.ofMillis(50))
                .start()
                .subscribe();

        node2 = TestNodeFactory.receive()
                .port(7001)
                .baseProbeTimeout(Duration.ofMillis(100))
                .baseProbeInterval(Duration.ofMillis(50))
                .start()
                .block();

        await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(node2.getCluster().getMembers()).containsExactlyInAnyOrder(7000, 7001);
        });
    }

}
