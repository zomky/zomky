package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.client.protobuf.InfoRequest;
import io.github.pmackowski.rsocket.raft.client.protobuf.InfoResponse;
import io.github.pmackowski.rsocket.raft.listener.SenderAvailableListener;
import io.github.pmackowski.rsocket.raft.listener.SenderUnavailableListener;
import io.github.pmackowski.rsocket.raft.storage.RaftStorage;
import io.github.pmackowski.rsocket.raft.storage.meta.Configuration;
import io.github.pmackowski.rsocket.raft.transport.Receiver;
import io.github.pmackowski.rsocket.raft.transport.Sender;
import io.github.pmackowski.rsocket.raft.transport.Senders;
import io.github.pmackowski.rsocket.raft.transport.protobuf.*;
import io.rsocket.Payload;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DefaultRaftServer implements InternalRaftServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultRaftServer.class);

    DefaultRaftServer() {}

    @Override
    public Mono<Void> onClose() {
        return null;
    }

    @Override
    public void dispose() {
        LOGGER.info("[RaftServer {}] Stopping ...", nodeId);
        raftGroups.onExit(this, raftStorage);

        receiver.stop();
        senders.stop();
    }

    StateMachine<ByteBuffer> stateMachine;
    StateMachineEntryConverter stateMachineEntryConverter;

    private Senders senders;
    private Set<SenderAvailableListener> senderAvailableListeners = new HashSet<>();
    private Set<SenderUnavailableListener> senderUnavailableListeners = new HashSet<>();
    private List<Integer> nodes = new ArrayList<>();

    int nodeId;

    private Receiver receiver;
    RaftStorage raftStorage;
    private boolean preVote;
    private boolean leaderStickiness;

    ElectionTimeout electionTimeout;

    private RaftGroups raftGroups;
    private Configuration initialConfiguration;
    private boolean passive;

    DefaultRaftServer(int port,
                      RaftStorage raftStorage,
                      Configuration initialConfiguration,
                      StateMachine<ByteBuffer> stateMachine,
                      StateMachineEntryConverter stateMachineEntryConverter,
                      ElectionTimeout electionTimeout,
                      boolean preVote,
                      boolean leaderStickiness,
                      boolean passive) {
        this.nodeId = port;
        this.raftStorage = raftStorage;
        this.stateMachine = stateMachine;
        this.stateMachineEntryConverter = stateMachineEntryConverter;
        this.electionTimeout = electionTimeout;
        this.preVote = preVote;
        this.leaderStickiness = leaderStickiness;
        this.initialConfiguration = initialConfiguration;
        nodes.addAll(initialConfiguration.getMembers());
        this.passive = passive;

        raftGroups = new RaftGroups(raftStorage, this);

        this.receiver = new Receiver(this, this.nodeId);
        this.senders = new Senders(this, this.nodeId);

        LOGGER.info("[RaftServer {}] has been initialized", nodeId);
    }

    public void start() {
        receiver.start();
        senders.start();

        raftGroups.start(this, raftStorage);
    }

    @Override
    public List<Integer> nodes() {
        return nodes;
    }

    public void startGroups() {
        raftGroups.start(this, raftStorage);
    }

    public Configuration getInitialConfiguration() {
        return initialConfiguration;
    }

//    @Override
    public boolean isPassive() {
        return passive;
    }

    public StateMachine<ByteBuffer> getStateMachine() {
        return stateMachine;
    }

    public StateMachineEntryConverter getStateMachineEntryConverter() {
        return stateMachineEntryConverter;
    }

    Mono<Sender> createSender(AddServerRequest addServerRequest) {
        return Mono.fromCallable(() -> Sender.createSender(addServerRequest.getNewServer())).cache().subscribeOn(Schedulers.elastic());
    }

    @Override
    public Mono<InfoResponse> onInfoRequest(InfoRequest infoRequest) {
//        return Mono.just(InfoResponse.newBuilder().addAllMembers(currentConfiguration.getMembers()).build());
        return Mono.empty();
    }

    @Override
    public Mono<Payload> onClientRequest(Payload payload) {
        return raftGroups.onClientRequest(this, raftStorage, payload);
    }

    @Override
    public Flux<Payload> onClientRequests(Publisher<Payload> payloads) {
        return raftGroups.onClientRequests(this, raftStorage, payloads);
    }

    @Override
    public ElectionTimeout getElectionTimeout() {
        return electionTimeout;
    }

    @Override
    public Mono<AppendEntriesResponse> onAppendEntries(String groupName, AppendEntriesRequest appendEntries) {
        return raftGroups.onAppendEntries(this, groupName, raftStorage, appendEntries);
    }

    @Override
    public Mono<PreVoteResponse> onPreRequestVote(String groupName, PreVoteRequest preRequestVote) {
        return raftGroups.onPreRequestVote(this, groupName, raftStorage, preRequestVote);
    }

    @Override
    public Mono<VoteResponse> onRequestVote(String groupName, VoteRequest requestVote) {
        return raftGroups.onRequestVote(this, groupName, raftStorage, requestVote);
    }

    public boolean preVote() {
        return preVote;
    }

    public boolean leaderStickiness() {
        return leaderStickiness;
    }

    @Override
    public Mono<AddServerResponse> onAddServer(String groupName, AddServerRequest addServerRequest) {
        return raftGroups.onAddServer(this, groupName, raftStorage, addServerRequest);
    }

    @Override
    public Mono<RemoveServerResponse> onRemoveServer(String groupName, RemoveServerRequest removeServerRequest) {
        return raftGroups.onRemoveServer(this, groupName, raftStorage, removeServerRequest);
    }

    @Override
    public void addGroup(RaftGroup raftGroup) {
        raftGroups.addGroup(raftGroup);
    }


    Flux<Sender> availableSenders() {
        return senders.availableSenders();
    }


    void onSenderAvailable(SenderAvailableListener senderAvailableListener) {
        senderAvailableListeners.add(senderAvailableListener);
    }

    void onSenderUnavailable(SenderUnavailableListener senderUnavailableListener) {
        senderUnavailableListeners.add(senderUnavailableListener);
    }

    //    @Override
    public void senderAvailable(Sender sender) {
        senderAvailableListeners.forEach(senderAvailableListener -> senderAvailableListener.handle(sender));
    }

    //    @Override
    public void senderUnavailable(Sender sender) {
        senderUnavailableListeners.forEach(senderUnavailableListener -> senderUnavailableListener.handle(sender));
    }
}
