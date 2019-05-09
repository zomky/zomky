package rsocket.playground.raft;

import com.google.protobuf.InvalidProtocolBufferException;
import io.rsocket.Payload;
import io.rsocket.util.ByteBufPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.retry.Repeat;
import rsocket.playground.raft.rpc.VoteRequest;
import rsocket.playground.raft.rpc.VoteResponse;
import rsocket.playground.raft.storage.LogEntryInfo;
import rsocket.playground.raft.storage.RaftStorage;
import rsocket.playground.raft.utils.NettyUtils;

public class CandidateRole implements RaftServerRole {

    private static final Logger LOGGER = LoggerFactory.getLogger(CandidateRole.class);

    private static final int QUORUM = 2; // hardcoded (assuming cluster of 3 nodes)

    private Disposable subscription;

    @Override
    public NodeState nodeState() {
        return NodeState.CANDIDATE;
    }

    @Override
    public void onInit(DefaultRaftServer node, RaftStorage raftStorage) {
        ElectionContext electionContext = new ElectionContext(false);
        subscription = Mono.defer(() -> startElection(node, raftStorage, electionContext))
                .repeatWhen(leaderNotElected(electionContext))
                .subscribe();
    }

    @Override
    public void onExit(DefaultRaftServer node, RaftStorage raftStorage) {
        subscription.dispose();
    }

    private Mono<Void> startElection(DefaultRaftServer node, RaftStorage raftStorage, ElectionContext electionContext) {
        return Mono.just(node)
                   .doOnNext(DefaultRaftServer::voteForMyself)
                   .then(sendVotes(node, raftStorage, electionContext));
    }

    private Mono<Void> sendVotes(DefaultRaftServer node, RaftStorage raftStorage, ElectionContext electionContext) {
        return node.availableSenders()
                .flatMap(sender -> sendVoteRequest(node, raftStorage, sender))
                .doOnNext(voteResponse -> {
                    if (voteResponse.getTerm() > raftStorage.getTerm()) {
                        node.convertToFollower(voteResponse.getTerm());
                        electionContext.setRepeatElection(false);
                    }
                })
                .filter(VoteResponse::getVoteGranted)
                // wait until quorum achieved or election timeout elapsed
                .buffer(QUORUM - 1)
                .timeout(node.electionTimeout.nextRandom())
                .next()
                .doOnSuccess(s -> {
                    node.convertToLeader();
                    electionContext.setRepeatElection(false);
                })
                .onErrorResume(throwable -> {
                    boolean repeatElection = !(throwable instanceof RaftException || subscription.isDisposed());
                    if (repeatElection) {
                        LOGGER.info("[RaftServer {}] Election timeout ({})", node.nodeId, subscription.isDisposed());
                        electionContext.setRepeatElection(repeatElection);
                    }
                    return Mono.empty();
                })
                .then();
    }

    private Mono<VoteResponse> sendVoteRequest(DefaultRaftServer node, RaftStorage raftStorage, Sender sender) {
        LogEntryInfo last = raftStorage.getLast();
        VoteRequest requestVote = VoteRequest.newBuilder()
                .setTerm(raftStorage.getTerm())
                .setCandidateId(node.nodeId)
                .setLastLogIndex(last.getIndex())
                .setLastLogTerm(last.getTerm())
                .build();

        Payload payload = ByteBufPayload.create(requestVote.toByteArray());
        if (node.nodeState.nodeState() != NodeState.CANDIDATE) {
            LOGGER.info("[RaftServer {} -> RaftServer {}] Vote dropped", node.nodeId, sender.getNodeId());
            return Mono.just(VoteResponse.newBuilder().setVoteGranted(false).build());
        }
        return sender.getRequestVoteSocket().requestResponse(payload)
                .map(payload1 -> {
                    try {
                        return VoteResponse.parseFrom(NettyUtils.toByteArray(payload1.sliceData()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid vote response!", e);
                    }
                })
                .onErrorResume(throwable -> {
                    LOGGER.error("[RaftServer {} -> RaftServer {}] Vote failure", node.nodeId, requestVote.getCandidateId(), throwable);
                    return Mono.just(VoteResponse.newBuilder().setVoteGranted(false).build());
                });
    }

    private Repeat<ElectionContext> leaderNotElected(ElectionContext electionContext) {
        return Repeat.<ElectionContext>onlyIf(repeatContext -> repeatContext.applicationContext().repeatElection())
                .withApplicationContext(electionContext);
    }

    private static class ElectionContext {

        private volatile boolean repeatElection;

        ElectionContext(boolean repeatElection) {
            this.repeatElection = repeatElection;
        }

        public void setRepeatElection(boolean repeatElection) {
            this.repeatElection = repeatElection;
        }

        public boolean repeatElection() {
            return repeatElection;
        }
    }

}