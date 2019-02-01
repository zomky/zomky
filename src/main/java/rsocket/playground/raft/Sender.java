package rsocket.playground.raft;

import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Sender {

    private static final Logger LOGGER = LoggerFactory.getLogger(Sender.class);

    private Node node;
    private List<Integer> clientPorts;

    private ConcurrentMap<Integer, RSocket> rsockets = new ConcurrentHashMap<>();

    public Sender(Node node, List<Integer> clientPorts) {
        this.clientPorts = clientPorts;
        this.node = node;
    }

    public void start() {
        senders().blockLast();
    }

    public void stop() {
        // TODO
    }

    Flux<RSocket> senders() {
        return sendersInternal()
                .doOnSubscribe(subscription -> LOGGER.info("on subscribe {}", node.nodeId))
                .subscribeOn(Schedulers.newElastic("sender-raft"));
    }

    Flux<RSocket> sendersInternal() {
        return Flux.create(emitter -> {
            clientPorts.forEach(clientPort -> {
                RSocket rSocket = rsockets.get(clientPort);
                if (rSocket != null) {
                    emitter.next(rSocket);
                    LOGGER.info("Node {} has access to {} (reused)", node.nodeId, clientPort);
                } else {
                    try {
                        rSocket = RSocketFactory.connect()
                                .transport(TcpClientTransport.create(clientPort))
                                .start()
                                .block();
                        rSocket.onClose().subscribe(v -> {
                            rsockets.remove(clientPort);
                            LOGGER.warn("Node {} has not longer access to {}", node.nodeId, clientPort);
                        });
                        LOGGER.info("Node {} has access to {}", node.nodeId, clientPort);
                        emitter.next(rSocket);
                        rsockets.putIfAbsent(clientPort, rSocket);
                    } catch (Exception e) {
                        LOGGER.warn("Node {} has no access to {}. Reason: {}", node.nodeId, clientPort, e.getMessage());
                    }
                }
            });
            emitter.complete();
            emitter.onCancel(() -> {
                //rsockets.values().forEach(Disposable::dispose);
            });
        });
    }

}
