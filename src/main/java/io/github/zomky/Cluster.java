package io.github.zomky;

import io.github.zomky.raft.RaftGroup;
import io.github.zomky.transport.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class Cluster {

    private static final Logger LOGGER = LoggerFactory.getLogger(Cluster.class);

    private int localNodeId;
    private Set<Integer> members;
    private ConcurrentMap<Integer, Sender> senders = new ConcurrentHashMap<>();
    private DirectProcessor<Sender> available;
    private FluxSink<Sender> availableSink;
    private DirectProcessor<Sender> unavailable;
    private FluxSink<Sender> unavailableSink;

    public Cluster(int nodeId) {
        this(nodeId, new HashSet<>());
    }

    public Cluster(int nodeId, Set<Integer> members) {
        this.localNodeId = nodeId;
        this.members = members;
        this.available = DirectProcessor.create();
        this.availableSink = available.sink();
        this.unavailable = DirectProcessor.create();
        this.unavailableSink = unavailable.sink();
        members.forEach(member -> senders.put(member, Sender.unavailableSender(member)));
    }

    public int getLocalNodeId() {
        return localNodeId;
    }

    public Set<Integer> getMembers() {
        return members;
    }

    public int membersCount() {
        return members.size();
    }

    public Set<Integer> allMembersExcept(int memberId) {
        return getMembers().stream().filter(member -> !member.equals(memberId)).collect(Collectors.toSet());
    }

    public void addMember(int member) {
        LOGGER.info("[Node {}] Cluster add member {}", localNodeId, member);
        this.members.add(member);
        doAvailableSender(member);

/*
        raftRsocket.onClose()
                .doFinally(signalType -> doUnavailableSender(member))
                .subscribe();
        doAvailableSender(member, raftRsocket);*/
    }

    public void removeMember(int member) {
        LOGGER.info("[Node {}] Cluster remove member {}", localNodeId, member);
        this.members.remove(member);
        doUnavailableSender(member);
    }

    public Flux<Sender> onSenderAvailable() {
        return available;
    }

    public Flux<Sender> onSenderUnavailable() {
        return unavailable;
    }

    public Mono<Sender> getSenderById(int nodeId) {
        return Mono.justOrEmpty(senders.get(nodeId));
    }

    // TODO refactoring
    public Sender senderById(int nodeId) {
        return senders.get(nodeId);
    }

    public Flux<Sender> availableSenders() {
        return Flux.create(emitter -> {
            // TODO emitter.requestedFromDownstream()
            senders.values().stream().filter(Sender::isAvailable).forEach(emitter::next);
            // lack of emitter.complete() is intentional
        });
    }

    public Flux<Sender> availableSenders(RaftGroup raftGroup) {
        return availableSenders(raftGroup.getCurrentConfiguration().allMembersExcept(localNodeId));
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

    private void doUnavailableSender(int member) {
        Sender sender = Sender.unavailableSender(member);
        LOGGER.warn("[Node {} -> Node {}] connection unavailable", localNodeId, member);
        senders.put(member, sender);
        unavailableSink.next(sender);
    }

    private void doAvailableSender(int member) {
        Sender sender = Sender.availableSender(member);
        LOGGER.info("[Node {} -> Node {}] connection available", localNodeId, member);
        senders.put(member, sender);
//        rsockets.put(member, RSocketFactory.connect()
//                .transport(TcpClientTransport.create(member))
//                .start()
//                .cache());

        availableSink.next(sender);
    }

}
