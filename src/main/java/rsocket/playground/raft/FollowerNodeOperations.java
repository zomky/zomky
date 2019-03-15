package rsocket.playground.raft;

import com.google.protobuf.ByteString;
import io.rsocket.Payload;
import io.rsocket.util.ByteBufPayload;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import rsocket.playground.raft.rpc.AppendEntriesRequest;
import rsocket.playground.raft.rpc.AppendEntriesResponse;
import rsocket.playground.raft.rpc.VoteRequest;
import rsocket.playground.raft.rpc.VoteResponse;
import rsocket.playground.raft.storage.LogEntryInfo;
import rsocket.playground.raft.storage.ZomkyStorage;

import java.nio.ByteBuffer;

public class FollowerNodeOperations implements NodeOperations {

    private static final Logger LOGGER = LoggerFactory.getLogger(FollowerNodeOperations.class);

    private DirectProcessor<Payload> processor;
    private FluxSink<Payload> sink;
    private Disposable subscription;

    public FollowerNodeOperations() {
        this.processor = DirectProcessor.create();
        this.sink = processor.sink(FluxSink.OverflowStrategy.BUFFER);
    }

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
        subscription = processor.timeout(node.electionTimeout.nextRandom())
                .subscribe(payload -> {}, throwable -> {
                    LOGGER.info("[Node {}] Election timeout ({})", node.nodeId, throwable.getMessage());
                    node.convertToCandidate();
                });
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
                       if (appendEntriesRequest.getTerm() > currentTerm) {
                           zomkyStorage.update(appendEntriesRequest.getTerm(), 0);
                       }

                       restartElectionTimer(node);

                       // 2.  Reply false if log doesn’t contain an entry at prevLogIndex
                       //     whose term matches prevLogTerm (§5.3)
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

                       return AppendEntriesResponse.newBuilder().setTerm(currentTerm).setSuccess(true).build();
                   });
    }


//    Raft determines which of two logs is more up-to-date
//    by comparing the index and term of the last entries in the
//    logs. If the logs have last entries with different terms, then
//    the log with the later term is more up-to-date. If the logs
//    end with the same term, then whichever log is longer is
//    more up-to-date.

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
                           zomkyStorage.update(requestVote.getTerm(), requestVote.getCandidateId());
                           restartElectionTimer(node);
                       }
                       return VoteResponse.newBuilder()
                               .setVoteGranted(voteGranted)
                               .setTerm(currentTerm)
                               .build();
                   });
    }

    private void restartElectionTimer(Node node) {
        LOGGER.debug("[Node {}] restartElectionTimer ...", node.nodeId);
        sink.next(ByteBufPayload.create(""));
    }

}
