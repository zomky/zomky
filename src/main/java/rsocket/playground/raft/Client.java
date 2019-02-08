package rsocket.playground.raft;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Iterator;
import java.util.List;

public class Client {

    private static final Logger LOGGER = LoggerFactory.getLogger(Node.class);

    private RSocket rSocket;
    private List<Integer> clientPorts;
    private int currentPort;

    public Client(List<Integer> clientPorts) {
        this.clientPorts = clientPorts;
    }

    public void start() {
        boolean rSocketInitialized = false;
        Iterator<Integer> iterator = clientPorts.iterator();
        // or maybe ask for leader in first step
        while (iterator.hasNext() && !rSocketInitialized) {
            Integer next = iterator.next() + 10000;
            try {
                rSocket = RSocketFactory.connect()
                        .transport(TcpClientTransport.create(next))
                        .start()
                        .block();
                rSocketInitialized = true;
                currentPort = next;
            } catch (Exception e) {
                LOGGER.warn("Server {} not available. Trying next one...", next, e);
            }
        }
        if (!rSocketInitialized) {
            LOGGER.error("Cluster is down!");
        }
    }

    public Mono<Integer> getLeaderId() {
        return Mono.just(1);
    }

    public Mono<Void> fireAndForget(Payload payload) {
        return rSocket.fireAndForget(payload);
    }

    public Mono<Payload> send(Payload payload) {
        return rSocket.requestResponse(payload);
    }

    public Flux<Payload> send(Publisher<Payload> payloads) {
        return rSocket.requestChannel(payloads);
    }
}
