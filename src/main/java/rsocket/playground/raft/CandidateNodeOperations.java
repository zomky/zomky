package rsocket.playground.raft;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.retry.Repeat;
import rsocket.playground.raft.rpc.AppendEntriesRequest;
import rsocket.playground.raft.rpc.AppendEntriesResponse;
import rsocket.playground.raft.rpc.VoteRequest;
import rsocket.playground.raft.rpc.VoteResponse;
import rsocket.playground.raft.storage.LogEntryInfo;
import rsocket.playground.raft.storage.ZomkyStorage;

import java.nio.ByteBuffer;

public class CandidateNodeOperations implements NodeOperations {

    private static final Logger LOGGER = LoggerFactory.getLogger(CandidateNodeOperations.class);

    private static final int QUORUM = 2; // hardcoded (assuming cluster of 3 nodes)

    private Disposable subscription;

    @Override
    public Mono<Payload> onClientRequest(Node node, ZomkyStorage zomkyStorage, Payload payload) {
        return Mono.error(new RaftException("I am not a leader!"));
    }

    @Override
    public Flux<Payload> onClientRequests(Node node, ZomkyStorage zomkyStorage, Publisher<Payload> payloads) {
        return Flux.error(new RaftException("I am not a leader!"));
    }

    @Override
    public void onInit(Node node, ZomkyStorage zomkyStorage) {
        ElectionContext electionContext = new ElectionContext(false);
        subscription = Mono.defer(() -> startElection(node, zomkyStorage, electionContext))
                .repeatWhen(leaderNotElected(electionContext))
                .subscribe();
    }

    @Override
    public void onExit(Node node, ZomkyStorage zomkyStorage) {
        subscription.dispose();
    }

    @Override
    public Mono<AppendEntriesResponse> onAppendEntries(Node node, ZomkyStorage zomkyStorage, AppendEntriesRequest appendEntries) {
        return Mono.just(appendEntries)
                   .map(appendEntriesRequest -> {
                       int currentTerm = zomkyStorage.getTerm();

                       // 1. Reply false if term < currentTerm (§5.1)
                       if (appendEntriesRequest.getTerm() < currentTerm) {
                           return AppendEntriesResponse.newBuilder().setTerm(currentTerm).setSuccess(false).build();
                       }

                       // 2. Reply false if log doesn’t contain an entry at index
                       //    whose term matches prevLogTerm (§5.3)
                       int prevLogTerm = zomkyStorage.getTermByIndex(appendEntriesRequest.getPrevLogIndex());
                       if (prevLogTerm != appendEntriesRequest.getPrevLogTerm()) {
                           return AppendEntriesResponse.newBuilder().setTerm(currentTerm).setSuccess(false).build();
                       }

                       // 3. If an existing entry conflicts with a new one (same index
                       //    but different terms), delete the existing entry and all that
                       //    follow it (§5.3)
                       if (zomkyStorage.getLast().getIndex() > appendEntriesRequest.getPrevLogIndex()) {
                           zomkyStorage.truncateFromIndex(appendEntriesRequest.getPrevLogIndex() + 1);
                       }
                       // 4. Append any new entries not already in the log
                       if (appendEntriesRequest.getEntries() != ByteString.EMPTY) {
                           zomkyStorage.appendLogs(ByteBuffer.wrap(appendEntriesRequest.getEntries().toByteArray()));
                       }

                       //5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
                       if (appendEntriesRequest.getLeaderCommit() > node.getCommitIndex()) {
                           node.setCommitIndex(Math.min(appendEntriesRequest.getLeaderCommit(), zomkyStorage.getLast().getIndex()));
                       }

                       if (appendEntriesRequest.getTerm() > currentTerm) { // >= ?
                           node.convertToFollower(appendEntriesRequest.getTerm());
                       }

                       return AppendEntriesResponse.newBuilder().setTerm(currentTerm).setSuccess(true).build();
                   });
    }

    @Override
    public Mono<VoteResponse> onRequestVote(Node node, ZomkyStorage zomkyStorage, VoteRequest requestVote) {
        return Mono.just(requestVote)
                   .map(requestVote1 -> {
                       int currentTerm = zomkyStorage.getTerm();
                       LogEntryInfo lastLogEntry = zomkyStorage.getLast();
                       // more up-to-date log
                       if (requestVote.getLastLogTerm() < lastLogEntry.getTerm() ||
                               (requestVote.getLastLogTerm() == lastLogEntry.getTerm() &&
                                       requestVote.getLastLogIndex() < lastLogEntry.getIndex())) {
                           return VoteResponse.newBuilder().setTerm(currentTerm).setVoteGranted(false).build();
                       }

                       if (requestVote.getTerm() < currentTerm) {
                           return VoteResponse.newBuilder().setTerm(currentTerm).setVoteGranted(false).build();
                       }

                       boolean voteGranted = node.notVoted(requestVote.getTerm());

                       if (voteGranted) {
                           node.convertToFollower(requestVote.getTerm());
                       }
                       return VoteResponse.newBuilder()
                               .setVoteGranted(voteGranted)
                               .setTerm(currentTerm)
                               .build();
                   });
    }

    private Mono<Void> startElection(Node node, ZomkyStorage zomkyStorage, ElectionContext electionContext) {
        return Mono.just(node)
                   .doOnNext(Node::voteForMyself)
                   .then(sendVotes(node, zomkyStorage, electionContext));
    }

    private Mono<Void> sendVotes(Node node, ZomkyStorage zomkyStorage, ElectionContext electionContext) {
        return node.availableSenders()
                .flatMap(sender -> sendVoteRequest(node, zomkyStorage, sender))
                .doOnNext(voteResponse -> {
                    if (voteResponse.getTerm() > zomkyStorage.getTerm()) {
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
                        LOGGER.info("[Node {}] Election timeout ({})", node.nodeId, subscription.isDisposed());
                        electionContext.setRepeatElection(repeatElection);
                    }
                    return Mono.empty();
                })
                .then();
    }

    private Mono<VoteResponse> sendVoteRequest(Node node, ZomkyStorage zomkyStorage, Sender sender) {
        LogEntryInfo last = zomkyStorage.getLast();
        VoteRequest requestVote = VoteRequest.newBuilder()
                .setTerm(zomkyStorage.getTerm())
                .setCandidateId(node.nodeId)
                .setLastLogIndex(last.getIndex())
                .setLastLogTerm(last.getTerm())
                .build();

        Payload payload = DefaultPayload.create(requestVote.toByteArray());
        if (node.nodeState != NodeState.CANDIDATE) {
            LOGGER.info("[Node {} -> Node {}] Vote dropped", node.nodeId, sender.getNodeId());
            return Mono.just(VoteResponse.newBuilder().setVoteGranted(false).build());
        }
        return sender.getRequestVoteSocket().requestResponse(payload)
                .map(payload1 -> {
                    try {
                        return VoteResponse.parseFrom(payload1.getData().array());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RaftException("Invalid vote response!", e);
                    }
                })
                .onErrorResume(throwable -> {
                    LOGGER.error("[Node {} -> Node {}] Vote failure", node.nodeId, requestVote.getCandidateId(), throwable);
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
