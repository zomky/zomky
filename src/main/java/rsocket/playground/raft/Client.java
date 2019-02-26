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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Client {

    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    private List<RSocket> rSockets = new ArrayList<>();
    private List<Integer> clientPorts;
    private RSocket leader;

    public Client(List<Integer> clientPorts) {
        this.clientPorts = clientPorts;
    }

    public void start() {
        boolean rSocketInitialized = false;
        Iterator<Integer> iterator = clientPorts.iterator();
        // or maybe ask for leader in first step
        while (iterator.hasNext() && !rSocketInitialized) {
            Integer next = iterator.next() + 20000;
            try {
                RSocket rSocket = RSocketFactory.connect()
                        .transport(TcpClientTransport.create(next))
                        .start()
                        .block();
                rSocketInitialized = true;
                rSockets.add(rSocket);
            } catch (Exception e) {
                LOGGER.warn("Server {} not available. Trying next one...", next, e);
            }
        }
        if (!rSocketInitialized) {
            LOGGER.error("Cluster is down!");
        } else {
            leader = rSockets.get(0);
        }
    }

    public Mono<Void> fireAndForget(Payload payload) {
        return leader.fireAndForget(payload);
    }

    public Mono<Payload> send(Payload payload) {
        return leader.requestResponse(payload);
    }

    public Flux<Payload> send(Publisher<Payload> payloads) {
        return Flux.defer(() -> leader.requestChannel(payloads))
                   .onErrorResume(t -> {
                       leader = rSockets.get(1);
                       return leader.requestChannel(payloads);
                   });
    }

    public Flux<Payload> receive(Payload payload) {
        return leader.requestStream(payload);
    }
}
