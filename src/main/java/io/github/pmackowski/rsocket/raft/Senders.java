package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.storage.meta.Configuration;
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

    private DefaultRaftServer raftServer;

    private ScheduledExecutorService executorService;

    private ConcurrentMap<Integer, Sender> senders = new ConcurrentHashMap<>();

    public Senders(DefaultRaftServer raftServer) {
        raftServer.getCurrentConfiguration().allMembersExcept(raftServer.nodeId).forEach(clientPort -> {
            senders.put(clientPort, Sender.unavailableSender(clientPort));
        });
        this.raftServer = raftServer;
    }

    public void addServer(int newMember) {
        senders.put(newMember, Sender.unavailableSender(newMember));
    }

    public void replaceWith(Configuration currentConfiguration) { // temporary, works only for add server
        currentConfiguration.getMembers().forEach(member -> {
            senders.putIfAbsent(member, Sender.unavailableSender(member));
        });
    }

    public void start() {
        executorService = Executors.newScheduledThreadPool(1);
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
                    LOGGER.debug("[RaftServer {}] no access to server {}", raftServer.nodeId, nodeId, e);
                }
            });
        }, 0, 100, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        executorService.shutdownNow();
        senders.values().forEach(sender -> {
            sender.stop();
            doUnavailableSender(sender.getNodeId());
        });
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
        LOGGER.warn("[RaftServer {} -> RaftServer {}] connection unavailable", raftServer.nodeId, nodeId);
        senders.put(nodeId, sender);
        raftServer.senderUnavailable(sender);
    }

    private void doAvailableSender(int nodeId, RSocket requestVoteSocket, RSocket appendEntriesSocket) {
        LOGGER.info("[RaftServer {} -> RaftServer {}] connection available", raftServer.nodeId, nodeId);
        Sender sender = Sender.availableSender(nodeId, requestVoteSocket, appendEntriesSocket);
        senders.put(nodeId, sender);
        raftServer.senderAvailable(sender);
    }

}
