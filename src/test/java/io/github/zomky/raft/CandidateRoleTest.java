package io.github.zomky.raft;

import io.github.zomky.Cluster;
import io.github.zomky.storage.InMemoryRaftStorage;
import io.github.zomky.storage.RaftStorage;
import io.github.zomky.transport.Sender;
import io.github.zomky.transport.protobuf.VoteRequest;
import io.github.zomky.transport.protobuf.VoteResponse;
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
    Cluster cluster;

    @Mock
    RaftGroup raftGroup;

    @Mock
    Sender sender1, sender2;

    RaftStorage raftStorage = new InMemoryRaftStorage();

    @Test
    void leaderElected() throws InterruptedException {
        given(raftGroup.quorum()).willReturn(2);
        given(raftGroup.nextElectionTimeout()).willReturn(Duration.ofMillis(100));
        given(raftGroup.isCandidate()).willReturn(true);
        given(raftGroup.availableSenders()).willReturn(Flux.just(sender1, sender2));

        VoteResponse voteResponse = voteGranted();

        given(sender1.requestVote(eq(raftGroup), any(VoteRequest.class)))
                .willReturn(Mono.just(voteResponse).delayElement(Duration.ofMillis(20)));
        given(sender2.requestVote(eq(raftGroup), any(VoteRequest.class)))
                .willReturn(Mono.just(voteResponse));

        candidateRole.onInit(cluster, raftGroup, raftStorage);

        Thread.sleep(150);
        verify(raftGroup).convertToLeader();
    }

    @Test
    void leaderElectedOneNode() {
        given(raftGroup.quorum()).willReturn(1);

        candidateRole.onInit(cluster, raftGroup, raftStorage);

        verify(raftGroup).voteForMyself();
        verify(raftGroup).convertToLeader();
    }

    @Test
    void leaderElectedQuorumReached() throws InterruptedException {
        given(raftGroup.quorum()).willReturn(2);
        given(raftGroup.nextElectionTimeout()).willReturn(Duration.ofMillis(100));
        given(raftGroup.isCandidate()).willReturn(true);
        given(raftGroup.availableSenders()).willReturn(Flux.just(sender1, sender2));

        given(sender1.requestVote(eq(raftGroup), any(VoteRequest.class)))
                .willReturn(Mono.just(voteGranted()).delayElement(Duration.ofMillis(20)));
        given(sender2.requestVote(eq(raftGroup), any(VoteRequest.class)))
                .willReturn(Mono.just(voteNotGranted()));

        candidateRole.onInit(cluster, raftGroup, raftStorage);

        Thread.sleep(150);
        verify(raftGroup).convertToLeader();
    }

    @Test
    void leaderElectedInSecondRound() throws InterruptedException {
        given(raftGroup.quorum()).willReturn(2);
        given(raftGroup.nextElectionTimeout()).willReturn(Duration.ofMillis(100));
        given(raftGroup.isCandidate()).willReturn(true);

        given(raftGroup.availableSenders()).willReturn(
                Flux.create(emitter -> {
                            emitter.next(sender1);
                            emitter.next(sender2);
                            // lack of emitter.complete() is intentional
                        }
                ));

        given(sender1.requestVote(eq(raftGroup), any(VoteRequest.class)))
                .willReturn(Mono.just(voteNotGranted()))
                .willReturn(Mono.just(voteGranted()).delayElement(Duration.ofMillis(70)));
        given(sender2.requestVote(eq(raftGroup), any(VoteRequest.class)))
                .willReturn(Mono.just(voteNotGranted()))
                .willReturn(Mono.just(voteGranted()).delayElement(Duration.ofMillis(70)));

        candidateRole.onInit(cluster, raftGroup, raftStorage);

        Thread.sleep(50);
        verify(raftGroup, times(1)).voteForMyself();
        Thread.sleep(100);
        verify(raftGroup, never()).convertToLeader();
        Thread.sleep(50);
        verify(raftGroup).convertToLeader();
        verify(raftGroup, times(2)).voteForMyself();
    }

    @Test
    void leaderElectedOneSenderGrantedVoteOtherIsHanging() throws InterruptedException {
        given(raftGroup.quorum()).willReturn(2);
        given(raftGroup.nextElectionTimeout()).willReturn(Duration.ofMillis(100));
        given(raftGroup.isCandidate()).willReturn(true);
        given(raftGroup.availableSenders()).willReturn(Flux.just(sender1, sender2));

        VoteResponse voteResponse = voteGranted();

        given(sender1.requestVote(eq(raftGroup), any(VoteRequest.class)))
                .willReturn(Mono.just(voteResponse).delayElement(Duration.ofMillis(20)));
        given(sender2.requestVote(eq(raftGroup), any(VoteRequest.class)))
                .willReturn(Mono.just(voteResponse).delayElement(Duration.ofSeconds(100)));

        candidateRole.onInit(cluster, raftGroup, raftStorage);

        Thread.sleep(150);
        verify(raftGroup).convertToLeader();
    }

    @Test
    void leaderNotElected() throws InterruptedException {
        given(raftGroup.quorum()).willReturn(2);
        given(raftGroup.nextElectionTimeout()).willReturn(Duration.ofMillis(100));
        given(raftGroup.isCandidate()).willReturn(true);
        given(raftGroup.availableSenders()).willReturn(
                Flux.create(emitter -> {
                    emitter.next(sender1);
                    emitter.next(sender2);
                    // lack of emitter.complete() is intentional
                }
        ));

        VoteResponse voteResponse = voteNotGranted();

        given(sender1.requestVote(eq(raftGroup), any(VoteRequest.class)))
                .willReturn(Mono.just(voteResponse).delayElement(Duration.ofMillis(20)));
        given(sender2.requestVote(eq(raftGroup), any(VoteRequest.class)))
                .willReturn(Mono.just(voteResponse));

        candidateRole.onInit(cluster, raftGroup, raftStorage);

        Thread.sleep(150);
        verify(raftGroup, never()).convertToLeader();
    }

    @Test
    void leaderNotElectedConvertToFollowerIfGreaterTerm() throws InterruptedException {
        given(raftGroup.quorum()).willReturn(2);
        given(raftGroup.nextElectionTimeout()).willReturn(Duration.ofMillis(100));
        given(raftGroup.isCandidate()).willReturn(true);
        given(raftGroup.availableSenders()).willReturn(
                Flux.create(emitter -> {
                            emitter.next(sender1);
                            emitter.next(sender2);
                            // lack of emitter.complete() is intentional
                        }
                ));

        given(sender1.requestVote(eq(raftGroup), any(VoteRequest.class)))
                .willReturn(Mono.just(voteNotGrantedGraterTerm()).delayElement(Duration.ofMillis(20)));
        given(sender2.requestVote(eq(raftGroup), any(VoteRequest.class)))
                .willReturn(Mono.just(voteNotGranted()));

        candidateRole.onInit(cluster, raftGroup, raftStorage);

        Thread.sleep(50);
        verify(raftGroup, never()).convertToLeader();
        verify(raftGroup).convertToFollower(1);
    }

    @Test
    void leaderNotElectedNoAvailableSenders() throws InterruptedException {
        given(raftGroup.quorum()).willReturn(2);
        given(raftGroup.nextElectionTimeout()).willReturn(Duration.ofMillis(10));
        given(raftGroup.availableSenders()).willReturn(Flux.create(emitter -> {}));

        candidateRole.onInit(cluster, raftGroup, raftStorage);

        Thread.sleep(50);
        verify(raftGroup, never()).convertToLeader();
        verify(raftGroup, atLeast(2)).voteForMyself();
    }

    @Test
    void leaderNotElectedAllSendersHanging() throws InterruptedException {
        given(raftGroup.quorum()).willReturn(2);
        given(raftGroup.nextElectionTimeout()).willReturn(Duration.ofMillis(50));
        given(raftGroup.isCandidate()).willReturn(true);
        given(raftGroup.availableSenders()).willReturn(
                Flux.create(emitter -> {
                            emitter.next(sender1);
                            emitter.next(sender2);
                            // lack of emitter.complete() is intentional
                        }
                ));

        VoteResponse voteResponse = voteGranted();

        given(sender1.requestVote(eq(raftGroup), any(VoteRequest.class)))
                .willReturn(Mono.just(voteResponse).delayElement(Duration.ofSeconds(100)));
        given(sender2.requestVote(eq(raftGroup), any(VoteRequest.class)))
                .willReturn(Mono.just(voteResponse).delayElement(Duration.ofSeconds(100)));

        candidateRole.onInit(cluster, raftGroup, raftStorage);

        Thread.sleep(120);
        verify(raftGroup, never()).convertToLeader();
        verify(raftGroup, atLeast(2)).voteForMyself();
    }

    @Test
    void leaderNotElectedAllSendersAreBroken() throws InterruptedException {
        given(raftGroup.quorum()).willReturn(2);
        given(raftGroup.nextElectionTimeout()).willReturn(Duration.ofMillis(50));
        given(raftGroup.isCandidate()).willReturn(true);
        given(raftGroup.availableSenders()).willReturn(
                Flux.create(emitter -> {
                            emitter.next(sender1);
                            emitter.next(sender2);
                            // lack of emitter.complete() is intentional
                        }
                ));

        given(sender1.requestVote(eq(raftGroup), any(VoteRequest.class)))
                .willReturn(Mono.error(new RuntimeException("sender 1 error")).delayElement(Duration.ofSeconds(100)).cast(VoteResponse.class));
        given(sender2.requestVote(eq(raftGroup), any(VoteRequest.class)))
                .willReturn(Mono.error(new RuntimeException("sender 2 error")).delayElement(Duration.ofSeconds(100)).cast(VoteResponse.class));

        candidateRole.onInit(cluster, raftGroup, raftStorage);

        Thread.sleep(120);
        verify(raftGroup, never()).convertToLeader();
        verify(raftGroup, atLeast(2)).voteForMyself();
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
