package io.github.zomky.raft;

import com.google.protobuf.ByteString;
import io.github.zomky.Cluster;
import io.github.zomky.storage.RaftStorage;
import io.github.zomky.storage.log.entry.IndexedLogEntry;
import io.github.zomky.storage.log.entry.IndexedTerm;
import io.github.zomky.storage.log.entry.LogEntry;
import io.github.zomky.storage.log.serializer.LogEntrySerializer;
import io.github.zomky.transport.protobuf.*;
import io.rsocket.Payload;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.Iterator;

public interface RaftRole {

    NodeState nodeState();

    void onInit(Cluster cluster, RaftGroup raftGroup, RaftStorage raftStorage);

    void onExit(Cluster cluster, RaftGroup raftGroup, RaftStorage raftStorage);

    default Mono<AppendEntriesResponse> onAppendEntries(Cluster cluster, RaftGroup raftGroup, RaftStorage raftStorage, AppendEntriesRequest appendEntries) {
        return Mono.just(appendEntries)
                .map(appendEntriesRequest -> {
                    int currentTerm = raftStorage.getTerm();
                    if (appendEntriesRequest.getTerm() > currentTerm) {
                        raftGroup.convertToFollower(appendEntriesRequest.getTerm());
                    }

                    // 1. Reply false if term < currentTerm
                    if (appendEntriesRequest.getTerm() < currentTerm) {
                        return AppendEntriesResponse.newBuilder().setTerm(currentTerm).setSuccess(false).build();
                    }

                    raftGroup.appendEntriesCall();

                    // 2.  Reply false if log doesn’t contain an entry at prevLogIndex
                    //     whose term matches prevLogTerm
                    int prevLogTerm = raftStorage.getTermByIndex(appendEntriesRequest.getPrevLogIndex());
                    if (prevLogTerm != appendEntriesRequest.getPrevLogTerm()) {
                        return AppendEntriesResponse.newBuilder().setTerm(currentTerm).setSuccess(false).build();
                    }

                    // 3. If an existing entry conflicts with a new one (same index
                    //    but different terms), delete the existing entry and all that
                    //    follow it
                    // 4. Append any new entries not already in the log
                    long lastIndex = raftStorage.getLastIndexedTerm().getIndex();
                    long currentIndex = appendEntriesRequest.getPrevLogIndex();

                    Iterator<ByteString> iterator = appendEntriesRequest.getEntriesList().iterator();
                    boolean truncated = false;
                    while (iterator.hasNext()) {
                        currentIndex++;
                        ByteString entry = iterator.next();
                        ByteBuffer byteBuffer = entry.asReadOnlyByteBuffer();
                        LogEntry logEntry = LogEntrySerializer.deserialize(byteBuffer);
                        if (currentIndex <= lastIndex && !truncated) {
                            if (logEntry.getTerm() == raftStorage.getTermByIndex(currentIndex)) {
                                continue;
                            } else {
                                raftStorage.truncateFromIndex(currentIndex);
                                truncated = true;
                            }
                        }
                        IndexedLogEntry indexedLogEntry = raftStorage.append(logEntry);
                        raftGroup.apply(indexedLogEntry);
                    }

                    // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
                    if (appendEntriesRequest.getLeaderCommit() > raftGroup.getCommitIndex()) {
                        raftGroup.setCommitIndex(Math.min(appendEntriesRequest.getLeaderCommit(), raftStorage.getLastIndexedTerm().getIndex()));
                    }

                    return AppendEntriesResponse.newBuilder().setTerm(currentTerm).setSuccess(true).build();
                });
    }

    default Mono<PreVoteResponse> onPreRequestVote(Cluster cluster, RaftGroup raftGroup, RaftStorage raftStorage, PreVoteRequest preRequestVote) {
        return Mono.just(preRequestVote)
                .map(requestVote1 -> {
                    int currentTerm = raftStorage.getTerm();
                    // 1. Reply false if last AppendEntries call was received
                    //    less than election timeout ago (leader stickiness)
                    if (raftGroup.isLeaderStickiness() && raftGroup.lastAppendEntriesWithinElectionTimeout()) {
                        return PreVoteResponse.newBuilder().setTerm(currentTerm).setVoteGranted(false).build();
                    }

                    // 2. Reply false if nextTerm < currentTerm
                    if (preRequestVote.getNextTerm() < currentTerm) {
                        return PreVoteResponse.newBuilder().setTerm(currentTerm).setVoteGranted(false).build();
                    }

                    IndexedTerm last = raftStorage.getLastIndexedTerm();
                    // 3. If caller's log is is at least as up-to-date as receiver's log, return true
                    // TODO move to IndexedTerm
                    if (preRequestVote.getLastLogTerm() < last.getTerm() ||
                            (preRequestVote.getLastLogTerm() == last.getTerm() &&
                                    preRequestVote.getLastLogIndex() < last.getIndex())) {
                        return PreVoteResponse.newBuilder().setTerm(currentTerm).setVoteGranted(false).build();
                    }
                    return PreVoteResponse.newBuilder().setTerm(currentTerm).setVoteGranted(true).build();

                });
    }

    default Mono<VoteResponse> onRequestVote(Cluster cluster, RaftGroup raftGroup, RaftStorage raftStorage, VoteRequest requestVote) {
        return Mono.just(requestVote)
                .map(requestVote1 -> {
                    int currentTerm = raftStorage.getTerm();

                    // 1. Reply false if last AppendEntries call was received
                    //    less than election timeout ago (leader stickiness)
                    if (raftGroup.isLeaderStickiness() && raftGroup.lastAppendEntriesWithinElectionTimeout()) {
                        return VoteResponse.newBuilder().setTerm(currentTerm).setVoteGranted(false).build();
                    }

                    if (requestVote.getTerm() > currentTerm) {
                        raftGroup.convertToFollower(requestVote.getTerm());
                    }

                    if (requestVote.getTerm() < currentTerm) {
                        return VoteResponse.newBuilder().setTerm(currentTerm).setVoteGranted(false).build();
                    }

                    IndexedTerm last = raftStorage.getLastIndexedTerm();

                    // Raft determines which of two logs is more up-to-date
                    // by comparing the index and term of the last entries in the
                    // logs. If the logs have last entries with different terms, then
                    // the log with the later term is more up-to-date. If the logs
                    // end with the same term, then whichever log is longer is
                    // more up-to-date.
                    if (requestVote.getLastLogTerm() < last.getTerm() ||
                            (requestVote.getLastLogTerm() == last.getTerm() &&
                                    requestVote.getLastLogIndex() < last.getIndex())) {
                        return VoteResponse.newBuilder().setTerm(currentTerm).setVoteGranted(false).build();
                    }

                    boolean voteGranted = requestVote.getTerm() > raftStorage.getTerm() ||
                            (requestVote.getTerm() == raftStorage.getTerm() && raftStorage.getVotedFor() == 0);

                    if (voteGranted) {
                        raftStorage.update(requestVote.getTerm(), requestVote.getCandidateId());
                        raftGroup.convertToFollower(requestVote.getTerm());
                    }
                    return VoteResponse.newBuilder()
                            .setVoteGranted(voteGranted)
                            .setTerm(currentTerm)
                            .build();
                });
    }

    default Mono<AddServerResponse> onAddServer(Cluster cluster, RaftGroup raftGroup, RaftStorage raftStorage, AddServerRequest addServerRequest) {
        return Mono.error(new RaftException(String.format("[Node %s] I am not a leader!", cluster.getLocalNodeId())));
    }

    default Mono<RemoveServerResponse> onRemoveServer(Cluster cluster, RaftGroup raftGroup, RaftStorage raftStorage, RemoveServerRequest removeServerRequest) {
        return Mono.error(new RaftException(String.format("[Node %s] I am not a leader!", cluster.getLocalNodeId())));
    }

    default Mono<Payload> onClientRequest(Cluster cluster, RaftGroup raftGroup, RaftStorage raftStorage, Payload payload) {
        return Mono.error(new RaftException(String.format("[Node %s] I am not a leader!", cluster.getLocalNodeId())));
    }

    default Flux<Payload> onClientRequests(Cluster cluster, RaftGroup raftGroup, RaftStorage raftStorage, Publisher<Payload> payloads) {
        return Flux.error(new RaftException(String.format("[Node %s] I am not a leader!", cluster.getLocalNodeId())));
    }

    default void markNewEntry(long index, long timestamp) {
        // no-op
    }
}
