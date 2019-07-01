package io.github.pmackowski.rsocket.raft;

import com.google.protobuf.ByteString;
import io.github.pmackowski.rsocket.raft.rpc.AppendEntriesRequest;
import io.github.pmackowski.rsocket.raft.rpc.PreVoteRequest;
import io.github.pmackowski.rsocket.raft.rpc.PreVoteResponse;
import io.github.pmackowski.rsocket.raft.rpc.VoteRequest;
import io.github.pmackowski.rsocket.raft.storage.InMemoryRaftStorage;
import io.github.pmackowski.rsocket.raft.storage.RaftStorage;
import io.github.pmackowski.rsocket.raft.storage.log.entry.CommandEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.LogEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.time.Duration;

import static io.github.pmackowski.rsocket.raft.storage.log.serializer.LogEntrySerializer.serialize;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FollowerRoleTest {

    FollowerRole followerRole = new FollowerRole();

    @Mock
    DefaultRaftServer node;

    @Mock
    Sender sender1, sender2;

    RaftStorage raftStorage = new InMemoryRaftStorage();

    //// ELECTION TIMEOUT ////

    @Test
    void electionTimeout() throws InterruptedException {
        Duration electionTimeout = Duration.ofMillis(50);
        given(node.nextElectionTimeout()).willReturn(electionTimeout);
        given(node.preVote()).willReturn(false);
        followerRole.onInit(node, raftStorage);

        int lag = 20;
        Thread.sleep(electionTimeout.toMillis() + lag);
        verify(node).convertToCandidate();
    }

    @Test
    void electionTimeoutWithPreVoteEnabled() throws InterruptedException {
        Duration electionTimeout = Duration.ofMillis(10);
        given(node.nextElectionTimeout()).willReturn(electionTimeout);
        given(node.preVote()).willReturn(true);
        given(node.availableSenders()).willReturn(Flux.just(sender1, sender2));

        PreVoteResponse preVoteResponse = PreVoteResponse
                .newBuilder()
                .setTerm(0)
                .setVoteGranted(true)
                .build();
        given(sender1.requestPreVote(any(PreVoteRequest.class)))
                .willReturn(Mono.just(preVoteResponse).delayElement(Duration.ofMillis(20)));
        given(sender2.requestPreVote(any(PreVoteRequest.class)))
                .willReturn(Mono.just(preVoteResponse));

        followerRole.onInit(node, raftStorage);

        int lag = 50;
        Thread.sleep(electionTimeout.toMillis() + lag);
        verify(node).convertToCandidate();
    }

    @Test
    void electionTimeoutWithPreVoteEnabledAndVoteNotGranted() throws InterruptedException {
        Duration electionTimeout = Duration.ofMillis(10);
        given(node.nextElectionTimeout()).willReturn(electionTimeout);
        given(node.preVote()).willReturn(true);
        given(node.availableSenders()).willReturn(Flux.just(sender1, sender2));

        PreVoteResponse preVoteResponse = PreVoteResponse
                .newBuilder()
                .setTerm(0)
                .setVoteGranted(false)
                .build();
        given(sender1.requestPreVote(any(PreVoteRequest.class)))
                .willReturn(Mono.just(preVoteResponse).delayElement(Duration.ofMillis(20)));
        given(sender2.requestPreVote(any(PreVoteRequest.class)))
                .willReturn(Mono.just(preVoteResponse));

        followerRole.onInit(node, raftStorage);

        int lag = 50;
        Thread.sleep(electionTimeout.toMillis() + lag);
        verify(node).refreshFollower();
    }

    @Test
    void noElectionTimeout() throws InterruptedException {
        Duration electionTimeout = Duration.ofMillis(50);
        given(node.nextElectionTimeout()).willReturn(electionTimeout);
        followerRole.onInit(node, raftStorage);

        long lessThanElectionTimeout = electionTimeout.toMillis() - 20;
        Thread.sleep(lessThanElectionTimeout);
        verify(node, never()).convertToCandidate();
    }

    @Test
    void onExitCallResetElectionTimeout() throws InterruptedException {
        given(node.nextElectionTimeout()).willReturn(Duration.ofMillis(50));
        followerRole.onInit(node, raftStorage);
        followerRole.onExit(node, raftStorage);

        Thread.sleep(100);
        verify(node, never()).convertToCandidate();
    }

    @Test
    void voteGrantedResetElectionTimeout() throws InterruptedException {
        given(node.nextElectionTimeout()).willReturn(Duration.ofMillis(100));

        VoteRequest voteRequest = VoteRequest.newBuilder()
                .setCandidateId(1)
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .setTerm(1)
                .build();

        followerRole.onInit(node, raftStorage);

        followerRole.onRequestVote(node, raftStorage, voteRequest)
                .delaySubscription(Duration.ofMillis(50))
                .subscribe();

        Thread.sleep(150);
        verify(node, never()).convertToCandidate();
    }

    @Test
    void validAppendEntriesResetElectionTimeout() throws InterruptedException {
        given(node.nextElectionTimeout()).willReturn(Duration.ofMillis(100));

        AppendEntriesRequest appendEntriesRequest = AppendEntriesRequest.newBuilder()
                .setPrevLogIndex(0)
                .setPrevLogTerm(0)
                .setTerm(1)
                .setLeaderCommit(1)
                .build();

        followerRole.onInit(node, raftStorage);

        followerRole.onAppendEntries(node, raftStorage, appendEntriesRequest)
                .delaySubscription(Duration.ofMillis(50))
                .subscribe();

        Thread.sleep(150);
        verify(node, never()).convertToCandidate();
        verify(node).appendEntriesCall();
    }

    //// REQUEST VOTE ////

    @Test
    void requestVoteBothCandidateAndFollowerHaveEmptyLog() {
        initFollower();

        VoteRequest voteRequest = VoteRequest.newBuilder()
                .setCandidateId(1)
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .setTerm(1)
                .build();

        StepVerifier.create(followerRole.onRequestVote(node, raftStorage, voteRequest))
                .assertNext(voteResponse -> {
                    assertThat(voteResponse.getTerm()).isEqualTo(0);
                    assertThat(voteResponse.getVoteGranted()).isEqualTo(true);
                }).verifyComplete();
    }

    @Test
    void requestVoteCandidateHasNotEmptyLog() {
        initFollower();

        VoteRequest voteRequest = VoteRequest.newBuilder()
                .setCandidateId(100)
                .setLastLogIndex(1)
                .setLastLogTerm(1)
                .setTerm(2)
                .build();

        StepVerifier.create(followerRole.onRequestVote(node, raftStorage, voteRequest))
                .assertNext(voteResponse -> {
                    assertThat(voteResponse.getTerm()).isEqualTo(0);
                    assertThat(voteResponse.getVoteGranted()).isEqualTo(true);
                    assertThat(raftStorage.getTerm()).isEqualTo(2);
                    assertThat(raftStorage.getVotedFor()).isEqualTo(100);
                    verify(node, times(2)).convertToFollower(2); // TODO should it be 1 invocation?

                }).verifyComplete();
    }

    @Test
    void requestVoteCandidateLogIsNotUpToDate() {
        initFollower();
        raftStorage.update(1, 0);
        raftStorage.append(commandEntry(1,  "val1"));

        VoteRequest voteRequest = VoteRequest.newBuilder()
                .setCandidateId(100)
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .setTerm(2)
                .build();

        StepVerifier.create(followerRole.onRequestVote(node, raftStorage, voteRequest))
                .assertNext(voteResponse -> {
                    assertThat(voteResponse.getTerm()).isEqualTo(1);
                    assertThat(voteResponse.getVoteGranted()).isEqualTo(false);
                    assertThat(raftStorage.getTerm()).isEqualTo(1);
                    assertThat(raftStorage.getVotedFor()).isEqualTo(0);
                    verify(node).convertToFollower(2);

                }).verifyComplete();
    }

    @Test
    void requestVoteCandidateLogIsNotUpToDateAndBothHaveSameTerm() {
        initFollower();
        raftStorage.update(2, 0);
        raftStorage.append(commandEntry(1,  "val1"));
        raftStorage.append(commandEntry(2,  "val2"));

        VoteRequest voteRequest = VoteRequest.newBuilder()
                .setCandidateId(100)
                .setLastLogIndex(1)
                .setLastLogTerm(2)
                .setTerm(2)
                .build();

        StepVerifier.create(followerRole.onRequestVote(node, raftStorage, voteRequest))
                .assertNext(voteResponse -> {
                    assertThat(voteResponse.getTerm()).isEqualTo(2);
                    assertThat(voteResponse.getVoteGranted()).isEqualTo(false);
                    assertThat(raftStorage.getTerm()).isEqualTo(2);
                    assertThat(raftStorage.getVotedFor()).isEqualTo(0);
                    verify(node, never()).convertToFollower(anyInt());

                }).verifyComplete();
    }

    @Test
    void requestVoteCandidateLogIsUpToDateAndBothHaveSameTerm() {
        initFollower();
        raftStorage.update(2, 0);
        raftStorage.append(commandEntry(1,  "val1"));
        raftStorage.append(commandEntry(2,  "val2"));

        VoteRequest voteRequest = VoteRequest.newBuilder()
                .setCandidateId(100)
                .setLastLogIndex(2)
                .setLastLogTerm(2)
                .setTerm(2)
                .build();

        StepVerifier.create(followerRole.onRequestVote(node, raftStorage, voteRequest))
                .assertNext(voteResponse -> {
                    assertThat(voteResponse.getTerm()).isEqualTo(2);
                    assertThat(voteResponse.getVoteGranted()).isEqualTo(true);
                    assertThat(raftStorage.getTerm()).isEqualTo(2);
                    assertThat(raftStorage.getVotedFor()).isEqualTo(100);
                    verify(node).convertToFollower(2);

                }).verifyComplete();
    }

    @Test
    void requestVoteCandidateLogIsUpToDateAndFollowerAlreadyVoted() {
        initFollower();
        raftStorage.update(2, 200);
        raftStorage.append(commandEntry(1,  "val1"));
        raftStorage.append(commandEntry(2,  "val2"));

        VoteRequest voteRequest = VoteRequest.newBuilder()
                .setCandidateId(100)
                .setLastLogIndex(2)
                .setLastLogTerm(2)
                .setTerm(2)
                .build();

        StepVerifier.create(followerRole.onRequestVote(node, raftStorage, voteRequest))
                .assertNext(voteResponse -> {
                    assertThat(voteResponse.getTerm()).isEqualTo(2);
                    assertThat(voteResponse.getVoteGranted()).isEqualTo(false);
                    assertThat(raftStorage.getTerm()).isEqualTo(2);
                    assertThat(raftStorage.getVotedFor()).isEqualTo(200);
                    verify(node, never()).convertToFollower(anyInt());
                }).verifyComplete();
    }

    @Test
    void requestVoteBothCandidateAndFollowerHaveEmptyLogAndFollowerAlreadyVoted() {
        initFollower();
        raftStorage.update(1, 200);

        VoteRequest voteRequest = VoteRequest.newBuilder()
                .setCandidateId(100)
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .setTerm(1)
                .build();

        StepVerifier.create(followerRole.onRequestVote(node, raftStorage, voteRequest))
                .assertNext(voteResponse -> {
                    assertThat(voteResponse.getTerm()).isEqualTo(1);
                    assertThat(voteResponse.getVoteGranted()).isEqualTo(false);
                }).verifyComplete();
    }

    @Test
    void requestVoteCandidateTermIsGreaterAndFollowerAlreadyVoted() {
        initFollower();
        raftStorage.update(1, 200);

        VoteRequest voteRequest = VoteRequest.newBuilder()
                .setCandidateId(100)
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .setTerm(2)
                .build();

        StepVerifier.create(followerRole.onRequestVote(node, raftStorage, voteRequest))
                .assertNext(voteResponse -> {
                    assertThat(voteResponse.getTerm()).isEqualTo(1);
                    assertThat(voteResponse.getVoteGranted()).isEqualTo(true);
                    assertThat(raftStorage.getTerm()).isEqualTo(2);
                    assertThat(raftStorage.getVotedFor()).isEqualTo(100);
                    verify(node, times(2)).convertToFollower(anyInt());
                }).verifyComplete();
    }

    //// REQUEST PRE-VOTE ////

    @Test
    void requestPreVoteLeaderStickiness() {
        initFollower();
        given(node.leaderStickiness()).willReturn(true);
        given(node.lastAppendEntriesWithinElectionTimeout()).willReturn(true);
        raftStorage.update(1, 0);

        PreVoteRequest preVoteRequest = PreVoteRequest.newBuilder()
                .setCandidateId(100)
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .setNextTerm(2)
                .build();

        StepVerifier.create(followerRole.onPreRequestVote(node, raftStorage, preVoteRequest))
                .assertNext(preVoteResponse -> {
                    assertThat(preVoteResponse.getTerm()).isEqualTo(1);
                    assertThat(preVoteResponse.getVoteGranted()).isEqualTo(false);
                    assertThat(raftStorage.getTerm()).isEqualTo(1);
                    assertThat(raftStorage.getVotedFor()).isEqualTo(0);
                    verify(node, never()).convertToFollower(anyInt());
                }).verifyComplete();
    }

    @Test
    void requestPreVoteLeaderStickinessButNoAppendEntriesCall() {
        initFollower();
        given(node.leaderStickiness()).willReturn(true);
        given(node.lastAppendEntriesWithinElectionTimeout()).willReturn(false);
        raftStorage.update(1, 0);

        PreVoteRequest preVoteRequest = PreVoteRequest.newBuilder()
                .setCandidateId(100)
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .setNextTerm(2)
                .build();

        StepVerifier.create(followerRole.onPreRequestVote(node, raftStorage, preVoteRequest))
                .assertNext(preVoteResponse -> {
                    assertThat(preVoteResponse.getTerm()).isEqualTo(1);
                    assertThat(preVoteResponse.getVoteGranted()).isEqualTo(true);
                    assertThat(raftStorage.getTerm()).isEqualTo(1);
                    assertThat(raftStorage.getVotedFor()).isEqualTo(0);
                    verify(node, never()).convertToFollower(anyInt());
                }).verifyComplete();
    }

    @Test
    void requestPreVoteNoLeaderStickinessCandidateNextTermSmallerThanFollowerCurrentTerm() {
        initFollower();
        given(node.leaderStickiness()).willReturn(false);
        raftStorage.update(3, 0);

        PreVoteRequest preVoteRequest = PreVoteRequest.newBuilder()
                .setCandidateId(100)
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .setNextTerm(2)
                .build();

        StepVerifier.create(followerRole.onPreRequestVote(node, raftStorage, preVoteRequest))
                .assertNext(preVoteResponse -> {
                    assertThat(preVoteResponse.getTerm()).isEqualTo(3);
                    assertThat(preVoteResponse.getVoteGranted()).isEqualTo(false);
                    assertThat(raftStorage.getTerm()).isEqualTo(3);
                    assertThat(raftStorage.getVotedFor()).isEqualTo(0);
                    verify(node, never()).convertToFollower(anyInt());
                }).verifyComplete();
    }

    @Test
    void requestPreVoteNoLeaderStickinessCandidateLogIsUpToDateAndBothHaveSameTerm() {
        initFollower();
        raftStorage.update(2, 0);
        raftStorage.append(commandEntry(1,  "val1"));
        raftStorage.append(commandEntry(2,  "val2"));

        PreVoteRequest preVoteRequest = PreVoteRequest.newBuilder()
                .setCandidateId(100)
                .setLastLogIndex(2)
                .setLastLogTerm(2)
                .setNextTerm(3)
                .build();

        StepVerifier.create(followerRole.onPreRequestVote(node, raftStorage, preVoteRequest))
                .assertNext(preVoteResponse -> {
                    assertThat(preVoteResponse.getTerm()).isEqualTo(2);
                    assertThat(preVoteResponse.getVoteGranted()).isEqualTo(true);
                    assertThat(raftStorage.getTerm()).isEqualTo(2);
                    assertThat(raftStorage.getVotedFor()).isEqualTo(0);
                    verify(node, never()).convertToFollower(anyInt());
                }).verifyComplete();
    }

    //// APPEND ENTRIES ////

    @Test
    void appendEntriesEmptyLog() {
        initFollower();
        raftStorage.update(0, 0);

        AppendEntriesRequest appendEntriesRequest = AppendEntriesRequest.newBuilder()
                .setPrevLogIndex(0)
                .setPrevLogTerm(0)
                .setTerm(1)
                .setLeaderCommit(1)
                .addEntries(entry(1, "val1"))
                .build();

        StepVerifier.create(followerRole.onAppendEntries(node, raftStorage, appendEntriesRequest))
                .assertNext(appendEntriesResponse -> {
                    assertThat(appendEntriesResponse.getTerm()).isEqualTo(0);
                    assertThat(appendEntriesResponse.getSuccess()).isEqualTo(true);
                    assertThat(raftStorage.getLastIndexedTerm().getIndex()).isEqualTo(1);
                    assertThat(((CommandEntry) raftStorage.getLastEntry().get().getLogEntry()).getValue()).isEqualTo("val1".getBytes());
                    verify(node).appendEntriesCall();
                    verify(node).setCommitIndex(1);
                    verify(node).convertToFollower(1);
                }).verifyComplete();
    }

    @Test
    void appendEntriesLogContainsEntries() {
        initFollower();
        raftStorage.update(1, 0);
        raftStorage.append(commandEntry(1,  "val1"));
        raftStorage.append(commandEntry(1,  "val2"));

        AppendEntriesRequest appendEntriesRequest = AppendEntriesRequest.newBuilder()
                .setPrevLogIndex(2)
                .setPrevLogTerm(1)
                .setTerm(2)
                .setLeaderCommit(3)
                .addEntries(entry(2, "val3"))
                .build();

        StepVerifier.create(followerRole.onAppendEntries(node, raftStorage, appendEntriesRequest))
                .assertNext(appendEntriesResponse -> {
                    assertThat(appendEntriesResponse.getTerm()).isEqualTo(1);
                    assertThat(appendEntriesResponse.getSuccess()).isEqualTo(true);
                    assertThat(raftStorage.getLastIndexedTerm().getIndex()).isEqualTo(3);
                    assertThat(((CommandEntry) raftStorage.getLastEntry().get().getLogEntry()).getValue()).isEqualTo("val3".getBytes());
                    verify(node).appendEntriesCall();
                    verify(node).setCommitIndex(3);
                    verify(node).convertToFollower(2);
                }).verifyComplete();
    }

    @Test
    void appendEntriesCommitIndexNoGreaterThanLogSize() {
        initFollower();
        raftStorage.update(1, 0);
        raftStorage.append(commandEntry(1,  "val1"));

        AppendEntriesRequest appendEntriesRequest = AppendEntriesRequest.newBuilder()
                .setPrevLogIndex(1)
                .setPrevLogTerm(1)
                .setTerm(1)
                .setLeaderCommit(20)
                .addEntries(entry(1, "val2")) // leader sends only one entry despite it could send more entries
                .build();

        StepVerifier.create(followerRole.onAppendEntries(node, raftStorage, appendEntriesRequest))
                .assertNext(appendEntriesResponse -> {
                    assertThat(appendEntriesResponse.getTerm()).isEqualTo(1);
                    assertThat(appendEntriesResponse.getSuccess()).isEqualTo(true);
                    verify(node).appendEntriesCall();
                    assertThat(raftStorage.getLastIndexedTerm().getIndex()).isEqualTo(2);
                    assertThat(((CommandEntry) raftStorage.getLastEntry().get().getLogEntry()).getValue()).isEqualTo("val2".getBytes());
                    verify(node).setCommitIndex(2); // not 20
                    verify(node, never()).convertToFollower(anyInt());
                }).verifyComplete();
    }

    @Test
    void appendEntriesLogContainsEntriesThaShouldBeTruncated() {
        initFollower();
        raftStorage.update(1, 0);
        raftStorage.append(commandEntry(1,  "val1"));
        raftStorage.append(commandEntry(1,  "val2"));
        raftStorage.append(commandEntry(1,  "val3"));

        AppendEntriesRequest appendEntriesRequest = AppendEntriesRequest.newBuilder()
                .setPrevLogIndex(1)
                .setPrevLogTerm(1)
                .setTerm(1)
                .setLeaderCommit(2)
                .addEntries(entry(1, "val2"))
                .build();

        StepVerifier.create(followerRole.onAppendEntries(node, raftStorage, appendEntriesRequest))
                .assertNext(appendEntriesResponse -> {
                    assertThat(appendEntriesResponse.getTerm()).isEqualTo(1);
                    assertThat(appendEntriesResponse.getSuccess()).isEqualTo(true);
                    verify(node).appendEntriesCall();
                    assertThat(raftStorage.getLastIndexedTerm().getIndex()).isEqualTo(2);
                    assertThat(((CommandEntry) raftStorage.getLastEntry().get().getLogEntry()).getValue()).isEqualTo("val2".getBytes());
                    verify(node).setCommitIndex(2);
                    verify(node, never()).convertToFollower(anyInt());
                }).verifyComplete();
    }

    @Test
    void appendEntriesLogContainsEntryTermsNotMatchingAtPreviousIndex() {
        initFollower();
        raftStorage.update(1, 0);
        raftStorage.append(commandEntry(1,  "val1"));
        raftStorage.append(commandEntry(1,  "val2"));

        AppendEntriesRequest appendEntriesRequest = AppendEntriesRequest.newBuilder()
                .setPrevLogIndex(2)
                .setPrevLogTerm(2) // greater term
                .setTerm(2)
                .setLeaderCommit(3)
                .addEntries(entry(2, "val3"))
                .build();

        StepVerifier.create(followerRole.onAppendEntries(node, raftStorage, appendEntriesRequest))
                .assertNext(appendEntriesResponse -> {
                    assertThat(appendEntriesResponse.getTerm()).isEqualTo(1);
                    assertThat(appendEntriesResponse.getSuccess()).isEqualTo(false);
                    verify(node).appendEntriesCall();
                    assertThat(raftStorage.getLastIndexedTerm().getIndex()).isEqualTo(2);
                    assertThat(((CommandEntry) raftStorage.getLastEntry().get().getLogEntry()).getValue()).isEqualTo("val2".getBytes());
                    verify(node, never()).setCommitIndex(3);
                    verify(node).convertToFollower(2);
                }).verifyComplete();
    }

    private void initFollower() {
        given(node.nextElectionTimeout()).willReturn(Duration.ofSeconds(100));
        followerRole.onInit(node, raftStorage);
    }

    private CommandEntry commandEntry(int term, String value) {
        return new CommandEntry(term, System.currentTimeMillis(), value.getBytes());
    }

    private ByteString entry(int term, String value) {
        CommandEntry commandEntry = commandEntry(term, value);
        ByteBuffer byteBuffer = ByteBuffer.allocate(LogEntry.SIZE + value.length() + 1);
        serialize(commandEntry, byteBuffer);
        byteBuffer.flip();

        return ByteString.copyFrom(byteBuffer);
    }
}