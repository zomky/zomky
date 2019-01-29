package rsocket.playground.raft;

import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import org.junit.Test;
import reactor.core.publisher.Flux;
import rsocket.playground.raft.transport.ObjectPayload;

import java.time.Duration;
import java.util.Arrays;

public class NodeTest {

    @Test
    public void name() throws InterruptedException {
        Node node1 = new Node(7000, Arrays.asList(7001));
        Node node2 = new Node(7001, Arrays.asList(7000));

        node1.start();
        node2.start();
        /*
        RSocket socket =
                RSocketFactory.connect()
                        .transport(TcpClientTransport.create("localhost", 7000))
                        .start()
                        .block();

        socket.requestChannel(Flux.interval(Duration.ofMillis(500)).map(i -> {
            RequestVote requestVote = new RequestVote().term(i);
            return ObjectPayload.create(requestVote, "2");
        })).subscribe();

        socket.requestChannel(Flux.interval(Duration.ofMillis(500)).map(i -> {
            RequestVote requestVote = new RequestVote().term(i);
            return ObjectPayload.create(requestVote, "2");
        })).subscribe();*/

        Thread.sleep(10000);
    }
}