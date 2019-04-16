package rsocket.playground.raft;

import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.concurrent.*;

public class Senders {

    private static final Logger LOGGER = LoggerFactory.getLogger(Senders.class);

    private Node node;

    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

    private ConcurrentMap<Integer, Sender> senders = new ConcurrentHashMap<>();

    public Senders(Node node, List<Integer> clientPorts) {
        clientPorts.forEach(clientPort -> {
            senders.put(clientPort, Sender.unavailableSender(clientPort));
        });
        this.node = node;
    }

    public void start() {
        executorService.scheduleAtFixedRate(() -> {
            senders.values().stream().filter(Sender::isNotAvailable).forEach(sender -> {
                int nodeId = sender.getNodeId();
                try {
                    RSocket requestVoteSocket = RSocketFactory.connect()
                            .transport(TcpClientTransport.create(nodeId))
                            .start()
                            .block();

                    RSocket appendEntriesSocket = RSocketFactory.connect()
                            .transport(TcpClientTransport.create(nodeId + 10000))
                            .start()
                            .block();

                    requestVoteSocket.onClose()
                            .doFinally(signalType -> doUnavailableSender(nodeId))
                            .subscribe();
                    appendEntriesSocket.onClose()
                            .doFinally(signalType -> doUnavailableSender(nodeId))
                            .subscribe();
                    doAvailableSender(nodeId, requestVoteSocket, appendEntriesSocket);
                } catch (Exception e) { // TODO more specific exceptions
                    LOGGER.debug("Node {} has no access to {}", node.nodeId, nodeId, e);
                }
            });
        }, 0, 100, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        executorService.shutdown();
    }


    Flux<Sender> availableSenders() {
        return Flux.create(emitter -> {
            senders.values().stream().filter(Sender::isAvailable).forEach(emitter::next);
            // lack of emitter.complete() is intentional
        });
    }

    Flux<Sender> unavailableSenders() {
        return Flux.defer(() -> Flux.fromIterable(senders.values())).filter(Sender::isNotAvailable);
    }

    private void doUnavailableSender(int nodeId) {
        Sender sender = Sender.unavailableSender(nodeId);
        LOGGER.warn("Node {} has no longer access to {}", node.nodeId, nodeId);
        senders.put(nodeId, sender);
        node.senderUnavailable(sender);
    }

    private void doAvailableSender(int nodeId, RSocket requestVoteSocket, RSocket appendEntriesSocket) {
        LOGGER.info("Node {} has access to {}", node.nodeId, nodeId);
        Sender sender = Sender.availableSender(nodeId, requestVoteSocket, appendEntriesSocket);
        senders.put(nodeId, sender);
        node.senderAvailable(sender);
    }

}
