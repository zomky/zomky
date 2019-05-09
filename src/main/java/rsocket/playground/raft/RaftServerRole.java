package rsocket.playground.raft;

import com.google.protobuf.ByteString;
import io.rsocket.Payload;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rsocket.playground.raft.rpc.AppendEntriesRequest;
import rsocket.playground.raft.rpc.AppendEntriesResponse;
import rsocket.playground.raft.rpc.VoteRequest;
import rsocket.playground.raft.rpc.VoteResponse;
import rsocket.playground.raft.storage.LogEntryInfo;
import rsocket.playground.raft.storage.RaftStorage;

import java.nio.ByteBuffer;

public interface RaftServerRole {

    NodeState nodeState();

    void onInit(DefaultRaftServer raftServer, RaftStorage raftStorage);

    void onExit(DefaultRaftServer raftServer, RaftStorage raftStorage);

    default Mono<AppendEntriesResponse> onAppendEntries(DefaultRaftServer node, RaftStorage raftStorage, AppendEntriesRequest appendEntries) {
        return Mono.just(appendEntries)
                .map(appendEntriesRequest -> {
                    int currentTerm = raftStorage.getTerm();
                    if (appendEntriesRequest.getTerm() > currentTerm) {
                        node.convertToFollower(appendEntriesRequest.getTerm());
                    }

                    // 1. Reply false if term < currentTerm
                    if (appendEntriesRequest.getTerm() < currentTerm) {
                        return AppendEntriesResponse.newBuilder().setTerm(currentTerm).setSuccess(false).build();
                    }

                    // 2.  Reply false if log doesn’t contain an entry at prevLogIndex
                    //     whose term matches prevLogTerm
                    int prevLogTerm = raftStorage.getTermByIndex(appendEntriesRequest.getPrevLogIndex());
                    if (prevLogTerm != appendEntriesRequest.getPrevLogTerm()) {
                        return AppendEntriesResponse.newBuilder().setTerm(currentTerm).setSuccess(false).build();
                    }

                    // 3. If an existing entry conflicts with a new one (same index
                    //    but different terms), delete the existing entry and all that
                    //    follow it
                    if (raftStorage.getLast().getIndex() > appendEntriesRequest.getPrevLogIndex()) {
                        raftStorage.truncateFromIndex(appendEntriesRequest.getPrevLogIndex() + 1);
                    }
                    // 4. Append any new entries not already in the log
                    if (appendEntriesRequest.getEntries() != ByteString.EMPTY) {
                        raftStorage.appendLogs(ByteBuffer.wrap(appendEntriesRequest.getEntries().toByteArray()));
                    }

                    //5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
                    if (appendEntriesRequest.getLeaderCommit() > node.getCommitIndex()) {
                        node.setCommitIndex(Math.min(appendEntriesRequest.getLeaderCommit(), raftStorage.getLast().getIndex()));
                    }

                    return AppendEntriesResponse.newBuilder().setTerm(currentTerm).setSuccess(true).build();
                });
    }

    default Mono<VoteResponse> onRequestVote(DefaultRaftServer node, RaftStorage raftStorage, VoteRequest requestVote) {
        return Mono.just(requestVote)
                .map(requestVote1 -> {
                    int currentTerm = raftStorage.getTerm();
                    if (requestVote.getTerm() > currentTerm) {
                        node.convertToFollower(requestVote.getTerm());
                    }

                    if (requestVote.getTerm() < currentTerm) {
                        return VoteResponse.newBuilder().setTerm(currentTerm).setVoteGranted(false).build();
                    }

                    LogEntryInfo lastLogEntry = raftStorage.getLast();

                    // Raft determines which of two logs is more up-to-date
                    // by comparing the index and term of the last entries in the
                    // logs. If the logs have last entries with different terms, then
                    // the log with the later term is more up-to-date. If the logs
                    // end with the same term, then whichever log is longer is
                    // more up-to-date.
                    if (requestVote.getLastLogTerm() < lastLogEntry.getTerm() ||
                            (requestVote.getLastLogTerm() == lastLogEntry.getTerm() &&
                                    requestVote.getLastLogIndex() < lastLogEntry.getIndex())) {
                        return VoteResponse.newBuilder().setTerm(currentTerm).setVoteGranted(false).build();
                    }

                    boolean voteGranted = node.notVoted(requestVote.getTerm());

                    if (voteGranted) {
                        raftStorage.update(requestVote.getTerm(), requestVote.getCandidateId());
                        node.convertToFollower(requestVote.getTerm());
                    }
                    return VoteResponse.newBuilder()
                            .setVoteGranted(voteGranted)
                            .setTerm(currentTerm)
                            .build();
                });
    }

    default Mono<Payload> onClientRequest(DefaultRaftServer raftServer, RaftStorage raftStorage, Payload payload) {
        return Mono.error(new RaftException(String.format("[RaftServer %s] I am not a leader!", raftServer.nodeId)));
    }

    default Flux<Payload> onClientRequests(DefaultRaftServer raftServer, RaftStorage raftStorage, Publisher<Payload> payloads) {
        return Flux.error(new RaftException(String.format("[RaftServer %s] I am not a leader!", raftServer.nodeId)));
    }

}
