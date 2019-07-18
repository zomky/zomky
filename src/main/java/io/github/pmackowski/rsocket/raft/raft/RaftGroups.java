package io.github.pmackowski.rsocket.raft.raft;

import io.github.pmackowski.rsocket.raft.InnerNode;
import io.github.pmackowski.rsocket.raft.external.statemachine.KVStateMachine1;
import io.github.pmackowski.rsocket.raft.external.statemachine.KVStateMachineEntryConverter;
import io.github.pmackowski.rsocket.raft.storage.meta.Configuration;
import io.github.pmackowski.rsocket.raft.transport.protobuf.*;
import io.rsocket.Payload;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RaftGroups {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftGroups.class);

    private InnerNode node;
    private ScheduledExecutorService stateMachineExecutor;
    private Map<String, RaftGroup> raftGroups = new ConcurrentHashMap<>();

    public RaftGroups(InnerNode node) {
        this.node = node;
        // TODO initialize from storage
    }

    public void start() {
        LOGGER.info("[Node {}] start groups", node.getNodeId());
        raftGroups.values().forEach(raftGroup -> raftGroup.onInit());
        stateMachineExecutor = Executors.newScheduledThreadPool(1);
        stateMachineExecutor.scheduleWithFixedDelay(() -> {
            try {
                raftGroups.values().forEach(raftGroup -> raftGroup.advanceStateMachine());
            } catch (Exception e) {
                LOGGER.error("Main loop failure", e);
            }
        }, 0, 10, TimeUnit.MILLISECONDS);

    }

    public void addGroup(RaftGroup raftGroup) {
        raftGroups.put(raftGroup.getGroupName(), raftGroup);
        raftGroup.onInit();
    }

    public void dispose() {
        raftGroups.values().forEach(raftGroup -> raftGroup.onExit());
        stateMachineExecutor.shutdownNow();
    }

    public Mono<Payload> onClientRequest(Payload payload) {
        String groupName = payload.getMetadataUtf8();
        RaftGroup raftGroup = raftGroups.get(groupName);
        return raftGroup.onClientRequest(payload);
    }

    public Flux<Payload> onClientRequests(Publisher<Payload> payloads) {
        return Flux.from(payloads)
                   .switchOnFirst(((signal, payloadFlux) -> {
                       Payload firstPayload = signal.get();
                       String groupName = firstPayload.getMetadataUtf8();
                       RaftGroup raftGroup = raftGroups.get(groupName);
                       return raftGroup.onClientRequests(payloadFlux);
                   }));
    }

    public Mono<AppendEntriesResponse> onAppendEntries(String groupName, AppendEntriesRequest appendEntries) {
        RaftGroup raftGroup = raftGroups.get(groupName);
        return raftGroup.onAppendEntries(appendEntries)
                .doOnNext(response -> {
                    if (response.getSuccess()) {
                        raftGroup.setCurrentLeader(appendEntries.getLeaderId());
                        if (appendEntries.getEntriesCount() > 0) {
                            LOGGER.info("[Node {} -> Node {}][group {}] Append entries \n{} \n-> \n{}", appendEntries.getLeaderId(), node.getNodeId(), raftGroup.getGroupName(), appendEntries, response);
                        }
                    }
                });
    }

    public Mono<PreVoteResponse> onPreRequestVote(String groupName, PreVoteRequest preRequestVote) {
        RaftGroup raftGroup = raftGroups.get(groupName);
        return raftGroup.onPreRequestVote(preRequestVote)
                .doOnNext(preVoteResponse -> LOGGER.info("[Node {} -> Node {}][group {}] Pre-Vote \n{} \n-> \n{}", preRequestVote.getCandidateId(), node.getNodeId(), raftGroup.getGroupName(), preRequestVote, preVoteResponse));

    }

    public Mono<VoteResponse> onRequestVote(String groupName, VoteRequest requestVote) {
        RaftGroup raftGroup = raftGroups.get(groupName);
        return raftGroup.onRequestVote(requestVote)
                .doOnNext(voteResponse -> LOGGER.info("[Node {} -> Node {}][group {}] Vote \n{} \n-> \n{}", requestVote.getCandidateId(), node.getNodeId(), raftGroup.getGroupName(), requestVote, voteResponse));
    }

    public Mono<AddServerResponse> onAddServer(String groupName, AddServerRequest addServerRequest) {
        RaftGroup raftGroup = raftGroups.get(groupName);
        return raftGroup.onAddServer(addServerRequest)
                .doOnNext(addServerResponse -> LOGGER.info("[Node {}][group {}] Add server \n{} \n-> \n{}", node.getNodeId(), raftGroup.getGroupName(), addServerRequest.getNewServer(), addServerResponse.getStatus()));
    }

    public Mono<RemoveServerResponse> onRemoveServer(String groupName, RemoveServerRequest removeServerRequest) {
        RaftGroup raftGroup = raftGroups.get(groupName);
        return raftGroup.onRemoveServer(removeServerRequest)
                .doOnNext(removeServerResponse -> LOGGER.info("[Node {}][group {}] Remove server \n{} \n-> \n{}", node.getNodeId(), raftGroup.getGroupName(), removeServerRequest.getOldServer(), removeServerResponse.getStatus()));
    }

    public Mono<AddGroupResponse> onCreateGroup(String groupName, AddGroupRequest createGroupRequest) {
        ElectionTimeout electionTimeout = createGroupRequest.getLeaderId() == node.getNodeId() ?
                ElectionTimeout.exactly(Duration.ofMillis(200)) : ElectionTimeout.exactly(Duration.ofMillis(1200));
        RaftRole raftRole = createGroupRequest.getNodesCount() == 0 ? new PassiveRole() : new FollowerRole();

        RaftGroup raftGroup = RaftGroup.builder()
                .groupName(groupName)
                .configuration(new Configuration(createGroupRequest.getNodesList()))
                .inMemoryRaftStorage() // TODO hardcoded
                .node(node)
                .raftRole(raftRole)
                .raftConfiguration(RaftConfiguration.builder()
                        .stateMachine(new KVStateMachine1(node.getNodeId())) // TODO hardcoded
                        .stateMachineEntryConverter(new KVStateMachineEntryConverter()) // TODO hardcoded
                        .electionTimeout(electionTimeout)
                        .build())
                .build();
        addGroup(raftGroup);
        return Mono.just(AddGroupResponse.newBuilder().setStatus(true).build())
                .doOnNext(createGroupResponse -> LOGGER.info("[Node {}] Create group {}", node.getNodeId(), groupName));
    }

    public void convertToFollower(int term) {

    }

    public RaftGroup getByName(String groupName) {
        return raftGroups.get(groupName);
    }
}