package io.github.pmackowski.rsocket.raft.transport;

import io.github.pmackowski.rsocket.raft.InnerNode;
import io.github.pmackowski.rsocket.raft.raft.RaftGroup;
import io.github.pmackowski.rsocket.raft.storage.meta.Configuration;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Senders {

    private static final Logger LOGGER = LoggerFactory.getLogger(Senders.class);

    private InnerNode node;

    private ScheduledExecutorService executorService;

    private ConcurrentMap<Integer, Sender> senders = new ConcurrentHashMap<>();

    public Senders(InnerNode node) {
        this.node = node;
        allMembersExcept(this.node.getNodeId()).forEach(clientPort -> {
            senders.put(clientPort, Sender.unavailableSender(clientPort));
        });
    }

    public Mono<Sender> getSenderById(int nodeId) {
        return Mono.justOrEmpty(senders.get(nodeId));
    }

    public Set<Integer> allMembersExcept(int memberId) {
        return this.node.getCluster().allMembersExcept(memberId);
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
            node.senderUnavailable(sender);
        }
    }

    public void replaceWith(Configuration currentConfiguration) {
        currentConfiguration.allMembersExcept(node.getNodeId()).forEach(member -> {
            senders.putIfAbsent(member, Sender.unavailableSender(member));
        });
        final Set<Integer> members = currentConfiguration.getMembers();
        final Set<Sender> toRemove = senders.values().stream().filter(sender -> !members.contains(sender.getNodeId())).collect(Collectors.toSet());
        toRemove.forEach(i -> removeServer(i.getNodeId()));
    }

    public void start() {
        executorService = Executors.newScheduledThreadPool(1);
        executorService.scheduleAtFixedRate(() -> {
            senders.values().stream()
                    .filter(sender -> sender.getNodeId() != node.getNodeId())
                    .filter(Sender::isNotAvailable)
                    .forEach(sender -> {
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
                            LOGGER.debug("[Node {}] no access to server {}", nodeId, nodeId, e);
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
            // TODO emitter.requestedFromDownstream()
            senders.values().stream().filter(Sender::isAvailable).forEach(emitter::next);
            // lack of emitter.complete() is intentional
        });
    }

    public Flux<Sender> availableSenders(RaftGroup raftGroup) {
        return availableSenders(raftGroup.getCurrentConfiguration().allMembersExcept(node.getNodeId()));
    }

    public Flux<Sender> availableSenders(Set<Integer> nodesSubset) {
        return Flux.create(emitter -> {
            // TODO emitter.requestedFromDownstream()
            senders.values().stream()
                    .filter(sender -> nodesSubset.contains(sender.getNodeId()))
                    .filter(Sender::isAvailable)
                    .forEach(emitter::next);
            // lack of emitter.complete() is intentional
        });
    }

    Flux<Sender> unavailableSenders() {
        return Flux.defer(() -> Flux.fromIterable(senders.values())).filter(Sender::isNotAvailable);
    }

    private void doUnavailableSender(int nodeId) {
        Sender sender = Sender.unavailableSender(nodeId);
        if (allMembersExcept(node.getNodeId()).contains(nodeId)) {
            LOGGER.warn("[Node {} -> Node {}] connection unavailable", node.getNodeId(), nodeId);
            senders.put(nodeId, sender);
        }
        node.senderUnavailable(sender);
    }

    private void doAvailableSender(int nodeId, RSocket raftRsocket) {
        Sender sender = Sender.availableSender(nodeId, raftRsocket);
        if (allMembersExcept(node.getNodeId()).contains(nodeId)) {
            LOGGER.info("[Node {} -> Node {}] connection available", node.getNodeId(), nodeId);
            senders.put(nodeId, sender);
        }
        node.senderAvailable(sender);
    }
}
