package io.github.pmackowski.rsocket.raft.transport;

import io.github.pmackowski.rsocket.raft.InternalRaftServer;
import io.github.pmackowski.rsocket.raft.storage.meta.Configuration;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Senders {

    private static final Logger LOGGER = LoggerFactory.getLogger(Senders.class);

    private InternalRaftServer raftServer;
    private int nodeId;

    private ScheduledExecutorService executorService;

    private ConcurrentMap<Integer, Sender> senders = new ConcurrentHashMap<>();

    public Senders(InternalRaftServer raftServer, int nodeId) {
        this.raftServer = raftServer;
        this.nodeId = nodeId;
        allMembersExcept(nodeId).forEach(clientPort -> {
            senders.put(clientPort, Sender.unavailableSender(clientPort));
        });
    }

    public Set<Integer> allMembersExcept(int memberId) {
        return raftServer.nodes().stream().filter(member -> !member.equals(memberId)).collect(Collectors.toSet());
    }

    public void addServer(int newMember) {
        final Sender sender = Sender.unavailableSender(newMember);
        senders.put(newMember, sender);
    }

    public void removeServer(int oldMember) {
        // TODO lock
        final Sender sender = senders.remove(oldMember);
        if (sender != null) {
            sender.stop();
            raftServer.senderUnavailable(sender);
        }
    }

    public void replaceWith(Configuration currentConfiguration) {
        currentConfiguration.allMembersExcept(nodeId).forEach(member -> {
            senders.putIfAbsent(member, Sender.unavailableSender(member));
        });
        final Set<Integer> members = currentConfiguration.getMembers();
        final Set<Sender> toRemove = senders.values().stream().filter(sender -> !members.contains(sender.getNodeId())).collect(Collectors.toSet());
        toRemove.forEach(i -> removeServer(i.getNodeId()));
    }

    public void start() {
        executorService = Executors.newScheduledThreadPool(1);
        executorService.scheduleAtFixedRate(() -> {
            senders.values().stream().filter(Sender::isNotAvailable).forEach(sender -> {
                int nodeId = sender.getNodeId();
                try {
                    RSocket raftRsocket = RSocketFactory.connect()
                            .transport(TcpClientTransport.create(nodeId))
                            .start()
                            .block();

                    raftRsocket.onClose()
                            .doFinally(signalType -> doUnavailableSender(nodeId))
                            .subscribe();
                    doAvailableSender(nodeId, raftRsocket);
                } catch (Exception e) { // TODO more specific exceptions
                    LOGGER.debug("[RaftServer {}] no access to server {}", nodeId, nodeId, e);
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


    public Flux<Sender> availableSenders() {
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
        LOGGER.warn("[RaftServer {} -> RaftServer {}] connection unavailable", nodeId, nodeId);
        if (allMembersExcept(this.nodeId).contains(nodeId)) {
            senders.put(nodeId, sender);
        }
        raftServer.senderUnavailable(sender);
    }

    private void doAvailableSender(int nodeId, RSocket raftRsocket) {
        LOGGER.info("[RaftServer {} -> RaftServer {}] connection available", nodeId, nodeId);
        Sender sender = Sender.availableSender(nodeId, raftRsocket);
        if (allMembersExcept(this.nodeId).contains(nodeId)) {
            senders.put(nodeId, sender);
        }
        raftServer.senderAvailable(sender);
    }
}
