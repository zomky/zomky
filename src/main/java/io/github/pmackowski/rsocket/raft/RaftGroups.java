package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.storage.InMemoryRaftStorage;
import io.github.pmackowski.rsocket.raft.storage.RaftStorage;
import io.github.pmackowski.rsocket.raft.transport.protobuf.*;
import io.rsocket.Payload;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RaftGroups {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftGroups.class);

    private RaftStorage raftStorage;
    int nodeId;

    private Map<String, RaftGroup> raftGroups = new ConcurrentHashMap<>();

    public RaftGroups(RaftStorage raftStorage, DefaultRaftServer node) {
//        this.raftStorage = raftStorage;
        this.nodeId = node.nodeId;
        // initialize from storage, suppose there are two raft groups
        raftGroups.put("group1", new RaftGroup(new InMemoryRaftStorage(), node, "group1"));
        raftGroups.put("group2", new RaftGroup(new InMemoryRaftStorage(), node, "group2"));
    }

    void start(DefaultRaftServer raftServer, RaftStorage raftStorage) {
        raftGroups.values().forEach(raftGroup -> raftGroup.onInit(raftServer));
    }

    void onExit(DefaultRaftServer raftServer, RaftStorage raftStorage) {
        raftGroups.values().forEach(raftGroup -> raftGroup.onExit(raftServer, raftStorage));
    }

    public Mono<Payload> onClientRequest(DefaultRaftServer defaultRaftServer, RaftStorage raftStorage, Payload payload) {
        String groupName = payload.getMetadataUtf8();
        RaftGroup raftGroup = raftGroups.get(groupName);
        return raftGroup.onClientRequest(defaultRaftServer, raftStorage, payload);
    }

    public Flux<Payload> onClientRequests(DefaultRaftServer defaultRaftServer, RaftStorage raftStorage, Publisher<Payload> payloads) {
        return Flux.from(payloads)
                   .switchOnFirst(((signal, payloadFlux) -> {
                       Payload firstPayload = signal.get();
                       String groupName = firstPayload.getMetadataUtf8();
                       RaftGroup raftGroup = raftGroups.get(groupName);
                       return raftGroup.onClientRequests(defaultRaftServer,raftStorage, payloadFlux);
                   }));
    }

    public Mono<AppendEntriesResponse> onAppendEntries(DefaultRaftServer defaultRaftServer, String groupName, RaftStorage raftStorage, AppendEntriesRequest appendEntries) {
        RaftGroup raftGroup = raftGroups.get(groupName);
        return raftGroup.onAppendEntries(defaultRaftServer, raftStorage, appendEntries)
                .doOnNext(response -> {
                    if (response.getSuccess()) {
                        raftGroup.setCurrentLeader(appendEntries.getLeaderId());
                        if (appendEntries.getEntriesCount() > 0) {
                            LOGGER.info("[RaftServer {} -> RaftServer {}] Append entries \n{} \n-> \n{}", appendEntries.getLeaderId(), nodeId, appendEntries, response);
                        }
                    }
                });
    }

    public Mono<PreVoteResponse> onPreRequestVote(DefaultRaftServer defaultRaftServer, String groupName, RaftStorage raftStorage, PreVoteRequest preRequestVote) {
        RaftGroup raftGroup = raftGroups.get(groupName);
        return raftGroup.onPreRequestVote(defaultRaftServer, raftStorage, preRequestVote)
                .doOnNext(preVoteResponse -> LOGGER.info("[RaftServer {} -> RaftServer {}] Pre-Vote \n{} \n-> \n{}", preRequestVote.getCandidateId(), nodeId, preRequestVote, preVoteResponse));

    }

    public Mono<VoteResponse> onRequestVote(DefaultRaftServer defaultRaftServer, String groupName, RaftStorage raftStorage, VoteRequest requestVote) {
        RaftGroup raftGroup = raftGroups.get(groupName);
        return raftGroup.onRequestVote(defaultRaftServer, raftStorage, requestVote)
                .doOnNext(voteResponse -> LOGGER.info("[RaftServer {} -> RaftServer {}] Vote \n{} \n-> \n{}", requestVote.getCandidateId(), nodeId, requestVote, voteResponse));
    }

    public Mono<AddServerResponse> onAddServer(DefaultRaftServer defaultRaftServer, String groupName, RaftStorage raftStorage, AddServerRequest addServerRequest) {
        RaftGroup raftGroup = raftGroups.get(groupName);
        return raftGroup.onAddServer(defaultRaftServer, raftStorage, addServerRequest)
                .doOnNext(addServerResponse -> LOGGER.info("[RaftServer {}] Add server \n{} \n-> \n{}", nodeId, addServerRequest.getNewServer(), addServerResponse.getStatus()));
    }

    public Mono<RemoveServerResponse> onRemoveServer(DefaultRaftServer defaultRaftServer, String groupName, RaftStorage raftStorage, RemoveServerRequest removeServerRequest) {
        RaftGroup raftGroup = raftGroups.get(groupName);
        return raftGroup.onRemoveServer(defaultRaftServer, raftStorage, removeServerRequest)
                .doOnNext(removeServerResponse -> LOGGER.info("[RaftServer {}] Remove server \n{} \n-> \n{}", nodeId, removeServerRequest.getOldServer(), removeServerResponse.getStatus()));

    }

    public void convertToFollower(int term) {

    }

    public RaftGroup getByName(String name) {
        return raftGroups.get(name);
    }
}
