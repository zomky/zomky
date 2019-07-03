package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.rpc.*;
import io.github.pmackowski.rsocket.raft.storage.RaftStorage;
import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedTerm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.time.Duration;

public class FollowerRole implements RaftServerRole {

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
    public void onInit(DefaultRaftServer node, RaftStorage raftStorage) {
        if (node.quorum() == 1) {
            node.convertToCandidate();
        } else {
            subscription = processor.timeout(node.nextElectionTimeout())
                    .onErrorResume(throwable -> {
                        LOGGER.info("[RaftServer {}] Election timeout ({})", node.nodeId, throwable.getMessage());
                        if (node.preVote() && node.quorum() - 1 > 0) {
                            return sendPreVotes(node, raftStorage)
                                    .doOnNext(preVotes -> {
                                        if (preVotes) {
                                            node.convertToCandidate();
                                        } else {
                                            node.refreshFollower();
                                        }
                                    })
                                    .then(Mono.empty());
                        } else {
                            node.convertToCandidate();
                            return Mono.empty();
                        }
                    })
                    .subscribe();
        }
    }

    @Override
    public void onExit(DefaultRaftServer node, RaftStorage raftStorage) {
        if (subscription != null) {
            subscription.dispose();
        }
    }

    @Override
    public Mono<VoteResponse> onRequestVote(DefaultRaftServer node, RaftStorage raftStorage, VoteRequest requestVote) {
        Mono<VoteResponse> voteResponse = RaftServerRole.super.onRequestVote(node, raftStorage, requestVote);
        return voteResponse.doOnNext(r -> {
            if (r.getVoteGranted()) {
                restartElectionTimer(node);
            }
        });
    }

    @Override
    public Mono<AppendEntriesResponse> onAppendEntries(DefaultRaftServer node, RaftStorage raftStorage, AppendEntriesRequest appendEntries) {
        return Mono.just(appendEntries)
                   .doOnNext(appendEntriesRequest -> {
                       int currentTerm = raftStorage.getTerm();
                       if (appendEntriesRequest.getTerm() >= currentTerm) {
                           restartElectionTimer(node);
                       }
                   }).then(RaftServerRole.super.onAppendEntries(node, raftStorage, appendEntries));
    }

    private void restartElectionTimer(DefaultRaftServer node) {
        try {
            if (subscription != null && !subscription.isDisposed()) {
                LOGGER.debug("[RaftServer {}] restartElectionTimer ...", node.nodeId);
                sink.next(System.currentTimeMillis());
            }
        } catch (Exception e) {
            LOGGER.error("[RaftServer {}] restartElectionTimer ... {}", node.nodeId, e);
        }
    }

    private Mono<Boolean> sendPreVotes(DefaultRaftServer node, RaftStorage raftStorage) {
        Duration timeout = node.nextElectionTimeout();
        return node.availableSenders()
                    .flatMap(sender -> sendPreVoteRequest(node, raftStorage, sender, timeout))
                    .filter(PreVoteResponse::getVoteGranted)
                    .buffer(node.quorum() - 1)
                    .timeout(timeout)
                    .next()
                    .map(i -> true)
                    .doOnError(throwable -> LOGGER.warn(String.format("[RaftServer %s] sendPreVote failed!", node.nodeId), throwable))
                    .onErrorReturn(false);
    }

    private Mono<PreVoteResponse> sendPreVoteRequest(DefaultRaftServer node, RaftStorage raftStorage, Sender sender, Duration timeout) {
        IndexedTerm last = raftStorage.getLastIndexedTerm();
        PreVoteRequest preVoteRequest = PreVoteRequest.newBuilder()
                .setNextTerm(raftStorage.getTerm() + 1)
                .setCandidateId(node.nodeId)
                .setLastLogIndex(last.getIndex())
                .setLastLogTerm(last.getTerm())
                .build();

        return sender.requestPreVote(preVoteRequest)
                     .timeout(timeout)
                     .onErrorResume(throwable -> {
                        LOGGER.error("[RaftServer {} -> RaftServer {}] Pre-Vote failure", node.nodeId, preVoteRequest.getCandidateId(), throwable);
                        return Mono.just(PreVoteResponse.newBuilder().setVoteGranted(false).build());
                     });
    }

}
