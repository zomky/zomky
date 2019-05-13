package io.github.pmackowski.rsocket.raft;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.pmackowski.rsocket.raft.rpc.*;
import io.github.pmackowski.rsocket.raft.storage.LogEntryInfo;
import io.github.pmackowski.rsocket.raft.storage.RaftStorage;
import io.github.pmackowski.rsocket.raft.utils.NettyUtils;
import io.rsocket.Payload;
import io.rsocket.util.ByteBufPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.time.Duration;

public class FollowerRole implements RaftServerRole {

    private static final Logger LOGGER = LoggerFactory.getLogger(FollowerRole.class);

    private static final int QUORUM = 2; // hardcoded (assuming cluster of 3 nodes)
    private static final boolean PRE_VOTE_ENABLED = true;

    private DirectProcessor<Payload> processor;
    private FluxSink<Payload> sink;
    private Disposable subscription;

    @Override
    public NodeState nodeState() {
        return NodeState.FOLLOWER;
    }

    @Override
    public void onInit(DefaultRaftServer node, RaftStorage raftStorage) {
        LOGGER.info("[Follower {}] onInit !!!!!", node.nodeId);
        processor = DirectProcessor.create();
        sink = processor.sink(FluxSink.OverflowStrategy.DROP);
        subscription = processor.timeout(node.electionTimeout.nextRandom())
                //.doOnNext(ReferenceCounted::release)
                .onErrorResume(throwable -> {
                    LOGGER.info("[RaftServer {}] Election timeout ({})", node.nodeId, throwable.getMessage());
                    if (PRE_VOTE_ENABLED) {
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

    @Override
    public void onExit(DefaultRaftServer node, RaftStorage raftStorage) {
        LOGGER.info("[Follower {}] onExit !!!!!", node.nodeId);
        subscription.dispose();
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
        LOGGER.debug("[RaftServer {}] restartElectionTimer ...", node.nodeId);
        try {
            if (!subscription.isDisposed()) {
                sink.next(ByteBufPayload.create(""));
            }
        } catch (Exception e) {
            LOGGER.error("[RaftServer {}] restartElectionTimer ... {}", node.nodeId, e);
        }
    }

    private Mono<Boolean> sendPreVotes(DefaultRaftServer node, RaftStorage raftStorage) {
        Duration timeout = node.electionTimeout.nextRandom();
        return node.availableSenders()
                    .flatMap(sender -> sendPreVoteRequest(node, raftStorage, sender, timeout))
                    .filter(PreVoteResponse::getVoteGranted)
                    .buffer(QUORUM - 1)
                    .timeout(timeout)
                    .next()
                    .map(i -> true)
                    .onErrorReturn(false);
    }

    private Mono<PreVoteResponse> sendPreVoteRequest(DefaultRaftServer node, RaftStorage raftStorage, Sender sender, Duration timeout) {
        LogEntryInfo last = raftStorage.getLast();
        PreVoteRequest preVoteRequest = PreVoteRequest.newBuilder()
                .setNextTerm(raftStorage.getTerm() + 1)
                .setCandidateId(node.nodeId)
                .setLastLogIndex(last.getIndex())
                .setLastLogTerm(last.getTerm())
                .build();

        Payload payload = ByteBufPayload.create(preVoteRequest.toByteArray(), "pre-vote".getBytes());
        return sender.getRequestVoteSocket().requestResponse(payload)
                .map(payload1 -> {
                    try {
                        return PreVoteResponse.parseFrom(NettyUtils.toByteArray(payload1.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid pre-vote response!", e);
                    }
                })
                .timeout(timeout)
                .onErrorResume(throwable -> {
                    LOGGER.error("[RaftServer {} -> RaftServer {}] Pre-Vote failure", node.nodeId, preVoteRequest.getCandidateId(), throwable);
                    return Mono.just(PreVoteResponse.newBuilder().setVoteGranted(false).build());
                });
    }

}
