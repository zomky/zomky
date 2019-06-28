package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.rpc.VoteRequest;
import io.github.pmackowski.rsocket.raft.rpc.VoteResponse;
import io.github.pmackowski.rsocket.raft.storage.InMemoryRaftStorage;
import io.github.pmackowski.rsocket.raft.storage.RaftStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class CandidateRoleTest {

    CandidateRole candidateRole = new CandidateRole();

    @Mock
    DefaultRaftServer node;

    @Mock
    Sender sender1, sender2;

    RaftStorage raftStorage = new InMemoryRaftStorage();

    @Test
    void leaderElected() throws InterruptedException {
        given(node.quorum()).willReturn(2);
        given(node.nextElectionTimeout()).willReturn(Duration.ofMillis(100));
        given(node.isCandidate()).willReturn(true);
        given(node.availableSenders()).willReturn(Flux.just(sender1, sender2));

        VoteResponse voteResponse = voteGranted();

        given(sender1.requestVote(any(VoteRequest.class)))
                .willReturn(Mono.just(voteResponse).delayElement(Duration.ofMillis(20)));
        given(sender2.requestVote(any(VoteRequest.class)))
                .willReturn(Mono.just(voteResponse));

        candidateRole.onInit(node, raftStorage);

        Thread.sleep(150);
        verify(node).convertToLeader();
    }

    @Test
    void leaderElectedQuorumReached() throws InterruptedException {
        given(node.quorum()).willReturn(2);
        given(node.nextElectionTimeout()).willReturn(Duration.ofMillis(100));
        given(node.isCandidate()).willReturn(true);
        given(node.availableSenders()).willReturn(Flux.just(sender1, sender2));

        given(sender1.requestVote(any(VoteRequest.class)))
                .willReturn(Mono.just(voteGranted()).delayElement(Duration.ofMillis(20)));
        given(sender2.requestVote(any(VoteRequest.class)))
                .willReturn(Mono.just(voteNotGranted()));

        candidateRole.onInit(node, raftStorage);

        Thread.sleep(150);
        verify(node).convertToLeader();
    }

    @Test
    void leaderElectedInSecondRound() throws InterruptedException {
        given(node.quorum()).willReturn(2);
        given(node.nextElectionTimeout()).willReturn(Duration.ofMillis(100));
        given(node.isCandidate()).willReturn(true);

        given(node.availableSenders()).willReturn(
                Flux.create(emitter -> {
                            emitter.next(sender1);
                            emitter.next(sender2);
                            // lack of emitter.complete() is intentional
                        }
                ));

        given(sender1.requestVote(any(VoteRequest.class)))
                .willReturn(Mono.just(voteNotGranted()))
                .willReturn(Mono.just(voteGranted()).delayElement(Duration.ofMillis(70)));
        given(sender2.requestVote(any(VoteRequest.class)))
                .willReturn(Mono.just(voteNotGranted()))
                .willReturn(Mono.just(voteGranted()).delayElement(Duration.ofMillis(70)));

        candidateRole.onInit(node, raftStorage);

        Thread.sleep(50);
        verify(node, times(1)).voteForMyself();
        Thread.sleep(100);
        verify(node, never()).convertToLeader();
        Thread.sleep(50);
        verify(node).convertToLeader();
        verify(node, times(2)).voteForMyself();
    }

    @Test
    void leaderElectedOneSenderGrantedVoteOtherIsHanging() throws InterruptedException {
        given(node.quorum()).willReturn(2);
        given(node.nextElectionTimeout()).willReturn(Duration.ofMillis(100));
        given(node.isCandidate()).willReturn(true);
        given(node.availableSenders()).willReturn(Flux.just(sender1, sender2));

        VoteResponse voteResponse = voteGranted();

        given(sender1.requestVote(any(VoteRequest.class)))
                .willReturn(Mono.just(voteResponse).delayElement(Duration.ofMillis(20)));
        given(sender2.requestVote(any(VoteRequest.class)))
                .willReturn(Mono.just(voteResponse).delayElement(Duration.ofSeconds(100)));

        candidateRole.onInit(node, raftStorage);

        Thread.sleep(150);
        verify(node).convertToLeader();
    }

    @Test
    void leaderNotElected() throws InterruptedException {
        given(node.quorum()).willReturn(2);
        given(node.nextElectionTimeout()).willReturn(Duration.ofMillis(100));
        given(node.isCandidate()).willReturn(true);
        given(node.availableSenders()).willReturn(
                Flux.create(emitter -> {
                    emitter.next(sender1);
                    emitter.next(sender2);
                    // lack of emitter.complete() is intentional
                }
        ));

        VoteResponse voteResponse = voteNotGranted();

        given(sender1.requestVote(any(VoteRequest.class)))
                .willReturn(Mono.just(voteResponse).delayElement(Duration.ofMillis(20)));
        given(sender2.requestVote(any(VoteRequest.class)))
                .willReturn(Mono.just(voteResponse));

        candidateRole.onInit(node, raftStorage);

        Thread.sleep(150);
        verify(node, never()).convertToLeader();
    }

    @Test
    void leaderNotElectedConvertToFollowerIfGreaterTerm() throws InterruptedException {
        given(node.quorum()).willReturn(2);
        given(node.nextElectionTimeout()).willReturn(Duration.ofMillis(100));
        given(node.isCandidate()).willReturn(true);
        given(node.availableSenders()).willReturn(
                Flux.create(emitter -> {
                            emitter.next(sender1);
                            emitter.next(sender2);
                            // lack of emitter.complete() is intentional
                        }
                ));

        given(sender1.requestVote(any(VoteRequest.class)))
                .willReturn(Mono.just(voteNotGrantedGraterTerm()).delayElement(Duration.ofMillis(20)));
        given(sender2.requestVote(any(VoteRequest.class)))
                .willReturn(Mono.just(voteNotGranted()));

        candidateRole.onInit(node, raftStorage);

        Thread.sleep(50);
        verify(node, never()).convertToLeader();
        verify(node).convertToFollower(1);
    }

    @Test
    void leaderNotElectedNoAvailableSenders() throws InterruptedException {
        given(node.quorum()).willReturn(2);
        given(node.nextElectionTimeout()).willReturn(Duration.ofMillis(10));
        given(node.availableSenders()).willReturn(Flux.create(emitter -> {}));

        candidateRole.onInit(node, raftStorage);

        Thread.sleep(50);
        verify(node, never()).convertToLeader();
        verify(node, atLeast(2)).voteForMyself();
    }

    @Test
    void leaderNotElectedAllSendersHanging() throws InterruptedException {
        given(node.quorum()).willReturn(2);
        given(node.nextElectionTimeout()).willReturn(Duration.ofMillis(50));
        given(node.isCandidate()).willReturn(true);
        given(node.availableSenders()).willReturn(
                Flux.create(emitter -> {
                            emitter.next(sender1);
                            emitter.next(sender2);
                            // lack of emitter.complete() is intentional
                        }
                ));

        VoteResponse voteResponse = voteGranted();

        given(sender1.requestVote(any(VoteRequest.class)))
                .willReturn(Mono.just(voteResponse).delayElement(Duration.ofSeconds(100)));
        given(sender2.requestVote(any(VoteRequest.class)))
                .willReturn(Mono.just(voteResponse).delayElement(Duration.ofSeconds(100)));

        candidateRole.onInit(node, raftStorage);

        Thread.sleep(120);
        verify(node, never()).convertToLeader();
        verify(node, atLeast(2)).voteForMyself();
    }

    @Test
    void leaderNotElectedAllSendersAreBroken() throws InterruptedException {
        given(node.quorum()).willReturn(2);
        given(node.nextElectionTimeout()).willReturn(Duration.ofMillis(50));
        given(node.isCandidate()).willReturn(true);
        given(node.availableSenders()).willReturn(
                Flux.create(emitter -> {
                            emitter.next(sender1);
                            emitter.next(sender2);
                            // lack of emitter.complete() is intentional
                        }
                ));

        given(sender1.requestVote(any(VoteRequest.class)))
                .willReturn(Mono.error(new RuntimeException("sender 1 error")).delayElement(Duration.ofSeconds(100)).cast(VoteResponse.class));
        given(sender2.requestVote(any(VoteRequest.class)))
                .willReturn(Mono.error(new RuntimeException("sender 2 error")).delayElement(Duration.ofSeconds(100)).cast(VoteResponse.class));

        candidateRole.onInit(node, raftStorage);

        Thread.sleep(120);
        verify(node, never()).convertToLeader();
        verify(node, atLeast(2)).voteForMyself();
    }

    private VoteResponse voteGranted() {
        return VoteResponse
                .newBuilder()
                .setTerm(0)
                .setVoteGranted(true)
                .build();
    }

    private VoteResponse voteNotGranted() {
        return VoteResponse
                .newBuilder()
                .setTerm(0)
                .setVoteGranted(false)
                .build();
    }

    private VoteResponse voteNotGrantedGraterTerm() {
        return VoteResponse
                .newBuilder()
                .setTerm(1)
                .setVoteGranted(false)
                .build();
    }
}
