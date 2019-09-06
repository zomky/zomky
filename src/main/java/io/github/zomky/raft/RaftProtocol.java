package io.github.zomky.raft;

import io.github.zomky.Cluster;
import io.github.zomky.InnerNode;
import io.github.zomky.storage.FileSystemRaftStorage;
import io.github.zomky.storage.InMemoryRaftStorage;
import io.github.zomky.storage.RaftStorage;
import io.github.zomky.storage.RaftStorageConfiguration;
import io.github.zomky.storage.log.SizeUnit;
import io.github.zomky.storage.meta.Configuration;
import io.github.zomky.transport.protobuf.*;
import io.rsocket.Payload;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RaftProtocol {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftProtocol.class);

    private Cluster cluster;
    private Map<String, RaftGroup> raftGroups = new ConcurrentHashMap<>();
    private Map<String, StateMachine> stateMachines = new HashMap<>();
    private Map<String, StateMachineEntryConverter> stateMachineConverters = new HashMap<>();

    public RaftProtocol(Cluster cluster) {
        this.cluster = cluster;
    }

    public void start() {
        LOGGER.info("[Node {}] Starting RAFT ...", cluster.getLocalNodeId());
        stateMachines.putAll(StateMachineUtils.stateMachines(cluster.getLocalNodeId()));
        stateMachineConverters.putAll(StateMachineUtils.stateMachineConverters());

        raftGroups.values().forEach(raftGroup -> raftGroup.onInit());
    }

    public void addGroup(RaftGroup raftGroup) {
        raftGroups.put(raftGroup.getGroupName(), raftGroup);
        raftGroup.onInit();
    }

    public void dispose() {
        raftGroups.values().forEach(raftGroup -> raftGroup.onExit());
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
                            LOGGER.debug("[Node {} -> Node {}][group {}] Append entries \n{} \n-> \n{}", appendEntries.getLeaderId(), cluster.getLocalNodeId(), raftGroup.getGroupName(), appendEntries, response);
                        }
                    }
                });
    }

    public Mono<PreVoteResponse> onPreRequestVote(String groupName, PreVoteRequest preRequestVote) {
        RaftGroup raftGroup = raftGroups.get(groupName);
        return raftGroup.onPreRequestVote(preRequestVote)
                .doOnNext(preVoteResponse -> LOGGER.debug("[Node {} -> Node {}][group {}] Pre-Vote \n{} \n-> \n{}", preRequestVote.getCandidateId(), cluster.getLocalNodeId(), raftGroup.getGroupName(), preRequestVote, preVoteResponse));

    }

    public Mono<VoteResponse> onRequestVote(String groupName, VoteRequest requestVote) {
        RaftGroup raftGroup = raftGroups.get(groupName);
        return raftGroup.onRequestVote(requestVote)
                .doOnNext(voteResponse -> LOGGER.debug("[Node {} -> Node {}][group {}] Vote \n{} \n-> \n{}", requestVote.getCandidateId(), cluster.getLocalNodeId(), raftGroup.getGroupName(), requestVote, voteResponse));
    }

    public Mono<AddServerResponse> onAddServer(String groupName, AddServerRequest addServerRequest) {
        RaftGroup raftGroup = raftGroups.get(groupName);
        return raftGroup.onAddServer(addServerRequest)
                .doOnNext(addServerResponse -> LOGGER.info("[Node {}][group {}] Add server \n{} \n-> \n{}", cluster.getLocalNodeId(), raftGroup.getGroupName(), addServerRequest.getNewServer(), addServerResponse.getStatus()));
    }

    public Mono<RemoveServerResponse> onRemoveServer(String groupName, RemoveServerRequest removeServerRequest) {
        RaftGroup raftGroup = raftGroups.get(groupName);
        return raftGroup.onRemoveServer(removeServerRequest)
                .doOnNext(removeServerResponse -> LOGGER.info("[Node {}][group {}] Remove server \n{} \n-> \n{}", cluster.getLocalNodeId(), raftGroup.getGroupName(), removeServerRequest.getOldServer(), removeServerResponse.getStatus()));
    }

    public Mono<AddGroupResponse> onAddGroup(String groupName, AddGroupRequest addGroupRequest) {
        RaftRole raftRole = Boolean.TRUE.equals(addGroupRequest.getPassive()) ? new PassiveRole() : new FollowerRole();
        ElectionTimeout electionTimeout = addGroupRequest.getLeaderIdSuggestion() == cluster.getLocalNodeId() ?
                ElectionTimeout.exactly(Math.min(300, addGroupRequest.getElectionTimeoutMin())) : // TODO extract 300
                ElectionTimeout.between(addGroupRequest.getElectionTimeoutMin(), addGroupRequest.getElectionTimeoutMax());

        RaftStorage raftStorage = Boolean.TRUE.equals(addGroupRequest.getPersistentStorage()) ?
                new FileSystemRaftStorage(RaftStorageConfiguration.builder()
                        .directoryGroup(groupName)
                        .segmentSize(SizeUnit.bytes, addGroupRequest.getSegmentSize())
                        .build()
                ) : new InMemoryRaftStorage();

        @SuppressWarnings("unchecked")
        RaftGroup raftGroup = RaftGroup.builder()
                .groupName(groupName)
                .raftStorage(raftStorage)
                .cluster(cluster)
                .raftRole(raftRole)
                .raftConfiguration(RaftConfiguration.builder()
                        .configuration(new Configuration(addGroupRequest.getNodesList()))
                        .stateMachine(stateMachines.get(addGroupRequest.getStateMachine()))
                        .stateMachineEntryConverter(stateMachineConverters.get(addGroupRequest.getStateMachine()))
                        .electionTimeout(electionTimeout)
                        .build())
                .build();
        addGroup(raftGroup);
        return Mono.just(AddGroupResponse.newBuilder().setStatus(true).build())
                .doOnNext(createGroupResponse -> LOGGER.debug("[Node {}] Create group {}", cluster.getLocalNodeId(), groupName));
    }

    public RaftGroup getByName(String groupName) {
        return raftGroups.get(groupName);
    }
}
