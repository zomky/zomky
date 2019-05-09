package rsocket.playground.raft;

import io.rsocket.Payload;
import io.rsocket.util.ByteBufPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import rsocket.playground.raft.rpc.AppendEntriesRequest;
import rsocket.playground.raft.rpc.AppendEntriesResponse;
import rsocket.playground.raft.rpc.VoteRequest;
import rsocket.playground.raft.rpc.VoteResponse;
import rsocket.playground.raft.storage.RaftStorage;

public class FollowerRole implements RaftServerRole {

    private static final Logger LOGGER = LoggerFactory.getLogger(FollowerRole.class);

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
                .subscribe(payload -> {}, throwable -> {
                    LOGGER.info("[RaftServer {}] Election timeout ({})", node.nodeId, throwable.getMessage());
                    node.convertToCandidate();
                });
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

}
