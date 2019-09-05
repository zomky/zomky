package io.github.zomky.raft;

import io.github.zomky.Cluster;
import io.github.zomky.storage.RaftStorage;
import io.github.zomky.storage.log.entry.IndexedTerm;
import io.github.zomky.transport.Sender;
import io.github.zomky.transport.protobuf.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.time.Duration;

public class FollowerRole implements RaftRole {

    private static final Logger LOGGER = LoggerFactory.getLogger(FollowerRole.class);

    private static DirectProcessor<Long> processor;
    private static FluxSink<Long> sink;

    static {
        // optimization
        processor = DirectProcessor.create();
        sink = processor.sink(FluxSink.OverflowStrategy.DROP);
    }

    private Disposable subscription;

    @Override
    public NodeState nodeState() {
        return NodeState.FOLLOWER;
    }

    @Override
    public void onInit(Cluster cluster, RaftGroup raftGroup, RaftStorage raftStorage) {
        if (raftGroup.quorum() == 1) {
            raftGroup.convertToCandidate();
        } else {
            subscription = processor.timeout(raftGroup.nextElectionTimeout())
                    .onErrorResume(throwable -> {
                        LOGGER.debug("[Node {}][group {}] Election timeout ({})", cluster.getLocalNodeId(), raftGroup.getGroupName(), throwable.getMessage());
                        if (raftGroup.isPreVote() && raftGroup.quorum() - 1 > 0) {
                            return sendPreVotes(cluster, raftGroup, raftStorage)
                                    .doOnNext(preVotes -> {
                                        if (preVotes) {
                                            raftGroup.convertToCandidate();
                                        } else {
                                            raftGroup.refreshFollower();
                                        }
                                    })
                                    .then(Mono.empty());
                        } else {
                            raftGroup.convertToCandidate();
                            return Mono.empty();
                        }
                    })
                    .subscribe();
        }
    }

    @Override
    public void onExit(Cluster cluster, RaftGroup raftGroup, RaftStorage raftStorage) {
        if (subscription != null) {
            subscription.dispose();
        }
    }

    @Override
    public Mono<VoteResponse> onRequestVote(Cluster cluster, RaftGroup raftGroup, RaftStorage raftStorage, VoteRequest requestVote) {
        Mono<VoteResponse> voteResponse = RaftRole.super.onRequestVote(cluster, raftGroup, raftStorage, requestVote);
        return voteResponse.doOnNext(r -> {
            if (r.getVoteGranted()) {
                restartElectionTimer(cluster);
            }
        });
    }

    @Override
    public Mono<AppendEntriesResponse> onAppendEntries(Cluster cluster, RaftGroup raftGroup, RaftStorage raftStorage, AppendEntriesRequest appendEntries) {
        return Mono.just(appendEntries)
                   .doOnNext(appendEntriesRequest -> {
                       int currentTerm = raftStorage.getTerm();
                       if (appendEntriesRequest.getTerm() >= currentTerm) {
                           restartElectionTimer(cluster);
                       }
                   }).then(RaftRole.super.onAppendEntries(cluster, raftGroup, raftStorage, appendEntries));
    }

    private void restartElectionTimer(Cluster cluster) {
        try {
            if (subscription != null && !subscription.isDisposed()) {
                LOGGER.debug("[Node {}] restartElectionTimer ...", cluster.getLocalNodeId());
                sink.next(System.currentTimeMillis());
            }
        } catch (Exception e) {
            LOGGER.error("[Node {}] restartElectionTimer ... {}", cluster.getLocalNodeId(), e);
        }
    }

    private Mono<Boolean> sendPreVotes(Cluster cluster, RaftGroup raftGroup, RaftStorage raftStorage) {
        Duration timeout = raftGroup.nextElectionTimeout();
        return raftGroup.availableSenders()
                    .flatMap(sender -> sendPreVoteRequest(cluster, raftGroup, raftStorage, sender, timeout))
                    .filter(PreVoteResponse::getVoteGranted)
                    .buffer(raftGroup.quorum() - 1)
                    .timeout(timeout)
                    .next()
                    .map(i -> true)
                    .doOnError(throwable -> LOGGER.warn(String.format("[Node %s][group %s] sendPreVote failed!", cluster.getLocalNodeId(), raftGroup.getGroupName()), throwable))
                    .onErrorReturn(false);
    }

    private Mono<PreVoteResponse> sendPreVoteRequest(Cluster cluster, RaftGroup raftGroup, RaftStorage raftStorage, Sender sender, Duration timeout) {
        IndexedTerm last = raftStorage.getLastIndexedTerm();
        PreVoteRequest preVoteRequest = PreVoteRequest.newBuilder()
                .setNextTerm(raftStorage.getTerm() + 1)
                .setCandidateId(cluster.getLocalNodeId())
                .setLastLogIndex(last.getIndex())
                .setLastLogTerm(last.getTerm())
                .build();

        return sender.requestPreVote(raftGroup, preVoteRequest)
                     .timeout(timeout)
                     .onErrorResume(throwable -> {
                        LOGGER.error("[Node {} -> Node {}][group {}] Pre-Vote failure", cluster.getLocalNodeId(), sender.getNodeId(), raftGroup.getGroupName(), throwable);
                        return Mono.just(PreVoteResponse.newBuilder().setVoteGranted(false).build());
                     });
    }

}
