package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.transport.protobuf.AddServerRequest;
import io.github.pmackowski.rsocket.raft.transport.protobuf.AppendEntriesRequest;
import io.github.pmackowski.rsocket.raft.transport.protobuf.AppendEntriesResponse;
import io.github.pmackowski.rsocket.raft.storage.InMemoryRaftStorage;
import io.github.pmackowski.rsocket.raft.storage.RaftStorage;
import io.github.pmackowski.rsocket.raft.storage.log.entry.CommandEntry;
import io.github.pmackowski.rsocket.raft.transport.Sender;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.verification.VerificationMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.retry.Repeat;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import static io.github.pmackowski.rsocket.raft.storage.log.serializer.LogEntrySerializer.deserialize;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class LeaderRoleTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderRoleTest.class);

    private static final Repeat<Object> NO_REPEAT = Repeat.onlyIf(objectRepeatContext -> false);

    LeaderRole leaderRole;

    @Mock
    Sender sender1, sender2;

    @Mock
    DefaultNode node;

    @Mock
    RaftGroup raftGroup;

    RaftStorage raftStorage = new InMemoryRaftStorage();

    @BeforeEach
    void setUp() {
        Mockito.lenient().when(sender1.getNodeId()).thenReturn(1);
        Mockito.lenient().when(sender2.getNodeId()).thenReturn(2);
        Mockito.lenient().when(raftGroup.quorum()).thenReturn(2);
    }

    @Test
    void leaderEmptyLog() {
        // given
        leaderRole = new LeaderRole(Repeat.times(0));
        raftStorage.update(1, 0);
        given(node.availableSenders()).willReturn(Flux.just(sender1));
        given(sender1.appendEntries(eq(raftGroup), any(AppendEntriesRequest.class))).willReturn(appendEntriesResponseSuccess(1));

        // when
        leaderRole.onInit(node, raftGroup, raftStorage);

        // then
        ArgumentCaptor<AppendEntriesRequest> argument = ArgumentCaptor.forClass(AppendEntriesRequest.class);
        verify(sender1).appendEntries(eq(raftGroup), argument.capture());
        AppendEntriesRequest actualAppendEntriesRequest = argument.getValue();
        assertThat(actualAppendEntriesRequest.getTerm()).isEqualTo(1);
        assertThat(actualAppendEntriesRequest.getPrevLogIndex()).isEqualTo(0);
        assertThat(actualAppendEntriesRequest.getEntriesCount()).isEqualTo(0);
    }

    @Test
    void leaderAtStartupAssumesFollowerHasAllEntries() {
        // given
        leaderRole = new LeaderRole(NO_REPEAT);
        raftStorage.update(1, 0);
        raftStorage.append(commandEntry(1,  "val1"));
        raftStorage.append(commandEntry(1,  "val2"));
        given(node.availableSenders()).willReturn(Flux.just(sender1));
        given(sender1.appendEntries(eq(raftGroup), any(AppendEntriesRequest.class))).willReturn(appendEntriesResponseSuccess(1));

        // when
        leaderRole.onInit(node, raftGroup, raftStorage);

        // then
        List<AppendEntriesRequest> appendEntriesHistory = appendEntriesHistory(times(1));
        assertAppendEntriesRequest(2, 0, appendEntriesHistory, 1);
    }

    @Test
    void leaderEmptyAppendEntry() throws InterruptedException {
        // given
        leaderRole = new LeaderRole(Repeat.once().fixedBackoff(Duration.ofMillis(20)));
        raftStorage.update(1, 0);
        given(node.availableSenders()).willReturn(Flux.just(sender1));
        given(sender1.appendEntries(eq(raftGroup), any(AppendEntriesRequest.class))).willReturn(appendEntriesResponseSuccess(1));
        leaderRole.onInit(node, raftGroup, raftStorage);

        // when
        raftStorage.append(commandEntry(1,  "val1"));
        raftStorage.append(commandEntry(1,  "val2"));

        // then
        Thread.sleep(50);

        List<AppendEntriesRequest> appendEntriesHistory = appendEntriesHistory(times(2));
        assertAppendEntriesRequest(0, 0, appendEntriesHistory, 1);
        assertAppendEntriesRequest(0, 2, appendEntriesHistory, 2);

        AppendEntriesRequest appendEntriesRequest = appendEntriesHistory.get(1);

        assertThat(appendEntriesRequest.getEntriesList().stream()
                .map(byteString -> deserialize(byteString.asReadOnlyByteBuffer(), CommandEntry.class))
                .map(CommandEntry::getValue)
                .map(String::new)
                .collect(Collectors.toList())).containsExactly("val1", "val2");
    }

    @Test
    void leaderLogNotEmptyAppendEntry() throws InterruptedException {
        // given
        leaderRole = new LeaderRole(Repeat.once().fixedBackoff(Duration.ofMillis(100)));
        raftStorage.update(1, 0);
        raftStorage.append(commandEntry(1,  "val1"));
        raftStorage.append(commandEntry(1,  "val2"));
        given(node.availableSenders()).willReturn(Flux.just(sender1));
        given(sender1.appendEntries(eq(raftGroup), any(AppendEntriesRequest.class))).willReturn(appendEntriesResponseSuccess(1));
        leaderRole.onInit(node, raftGroup, raftStorage);

        // when
        raftStorage.append(commandEntry(1,  "val3"));

        // then
        Thread.sleep(120);

        List<AppendEntriesRequest> appendEntriesHistory = appendEntriesHistory(times(2));
        assertAppendEntriesRequest(2, 0, appendEntriesHistory, 1);
        assertAppendEntriesRequest(2, 1, appendEntriesHistory, 2);

        AppendEntriesRequest appendEntriesRequest = appendEntriesHistory.get(1);

        assertThat(appendEntriesRequest.getEntriesList().stream()
                .map(byteString -> deserialize(byteString.asReadOnlyByteBuffer(), CommandEntry.class))
                .map(CommandEntry::getValue)
                .map(String::new)
                .collect(Collectors.toList())).containsExactly("val3");
    }

    @Test
    @DisplayName("Joining server log is empty")
    void onAddServers() {
        // given
        given(node.createSender(any())).willReturn(Mono.just(sender1));
        when(sender1.appendEntries(eq(raftGroup), any(AppendEntriesRequest.class)))
                .thenReturn(appendEntriesResponse(false, 0, Duration.ofMillis(100)))  // not counted as round
                .thenReturn(appendEntriesResponse(true, 0, Duration.ofMillis(100)))   // round 1
                .thenReturn(appendEntriesResponse(true, 0, Duration.ofMillis(100)));  // round 2

        int catchUpMaxRounds = 2;

        leaderRole = new LeaderRole(catchUpMaxRounds);
        raftStorage.update(1, 0);
        raftStorage.append(commandEntry(1,  "val1"));
        raftStorage.append(commandEntry(1,  "val2"));

        // when
        StepVerifier.create(leaderRole.onAddServer(node, raftGroup, raftStorage, addServerRequest()))
                .thenAwait(Duration.ofMillis(200))
                .then(() -> {
                    raftStorage.append(commandEntry(2,  "val3"));
                    raftStorage.append(commandEntry(2,  "val3"));
                    raftStorage.append(commandEntry(2,  "val3"));
                    raftStorage.append(commandEntry(2,  "val3"));
                    raftStorage.append(commandEntry(2,  "val3"));
                })
                .assertNext(addServerResponse -> {
                    assertThat(addServerResponse.getStatus()).isTrue();
//                    assertThat(addServerResponse.getLeaderHint()).isEqualTo(7003);
                })
                .verifyComplete();

        List<AppendEntriesRequest> appendEntriesHistory = appendEntriesHistory(times(catchUpMaxRounds + 1));
        assertAppendEntriesRequest(2, 0, appendEntriesHistory, 1);
        assertAppendEntriesRequest(0, 2, appendEntriesHistory, 2);
        assertAppendEntriesRequest(2, 5, appendEntriesHistory, 3);

    }

    private AddServerRequest addServerRequest() {
        return AddServerRequest.newBuilder().setNewServer(7003).build();
    }

    private List<AppendEntriesRequest> appendEntriesHistory() {
        return appendEntriesHistory(atLeastOnce());
    }

    private List<AppendEntriesRequest> appendEntriesHistory(VerificationMode verificationMode) {
        ArgumentCaptor<AppendEntriesRequest> argument = ArgumentCaptor.forClass(AppendEntriesRequest.class);
        verify(sender1, verificationMode).appendEntries(eq(raftGroup), argument.capture());
        return argument.getAllValues();
    }

    private void assertAppendEntriesRequest(long prevlogIndex, int entriesCount, List<AppendEntriesRequest> appendEntriesHistory, int position) {
        AppendEntriesRequest appendEntriesRequest = appendEntriesHistory.get(position-1);
        //assertThat(appendEntriesRequest.getTerm()).isEqualTo(1);
        assertThat(appendEntriesRequest.getPrevLogIndex()).isEqualTo(prevlogIndex);
        assertThat(appendEntriesRequest.getEntriesCount()).isEqualTo(entriesCount);
    }

    private Mono<AppendEntriesResponse> appendEntriesResponse(boolean success, int term, long lastLogIndex) {
        return appendEntriesResponse(success, term, lastLogIndex, null);
    }

    private Mono<AppendEntriesResponse> appendEntriesResponse(boolean success, long lastLogIndex) {
        return appendEntriesResponse(success, 1, lastLogIndex, null);
    }

    private Mono<AppendEntriesResponse> appendEntriesResponse(boolean success, long lastLogIndex, Duration delay) {
        return appendEntriesResponse(success, 1, lastLogIndex, delay);
    }

    private Mono<AppendEntriesResponse> appendEntriesResponse(boolean success, int term, long lastLogIndex, Duration delay) {
        Mono result = Mono.just(AppendEntriesResponse
                            .newBuilder()
                            .setSuccess(success)
                            .setTerm(term)
                            .setLastLogIndex(lastLogIndex)
                            .build()
                    );
        if (delay != null) {
            result = result.delayElement(delay);
        }
        return result;
    }

    private Mono<AppendEntriesResponse> appendEntriesResponseSuccess(int term) {
        return Mono.just(AppendEntriesResponse
                .newBuilder()
                .setSuccess(true)
                .setTerm(term)
                .build());
    }

    private Mono<AppendEntriesResponse> appendEntriesResponseNotSuccess(int term) {
        return Mono.just(AppendEntriesResponse
                .newBuilder()
                .setSuccess(false)
                .setTerm(term)
                .build());
    }

    private CommandEntry commandEntry(int term, String value) {
        return new CommandEntry(term, System.currentTimeMillis(), value.getBytes());
    }
}
