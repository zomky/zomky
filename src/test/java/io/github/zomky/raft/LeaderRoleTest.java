package io.github.zomky.raft;

import io.github.zomky.Cluster;
import io.github.zomky.storage.InMemoryRaftStorage;
import io.github.zomky.storage.RaftStorage;
import io.github.zomky.storage.log.entry.CommandEntry;
import io.github.zomky.transport.Sender;
import io.github.zomky.transport.protobuf.AddServerRequest;
import io.github.zomky.transport.protobuf.AppendEntriesRequest;
import io.github.zomky.transport.protobuf.AppendEntriesResponse;
import org.junit.jupiter.api.*;
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
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import static io.github.zomky.storage.log.serializer.LogEntrySerializer.deserialize;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class LeaderRoleTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderRoleTest.class);

    LeaderRole leaderRole;

    @Mock
    Sender sender1, sender2;

    @Mock
    Cluster cluster;

    @Mock
    RaftGroup raftGroup;

    RaftStorage raftStorage = new InMemoryRaftStorage();

    @BeforeEach
    void setUp() {
        Mockito.lenient().when(sender1.getNodeId()).thenReturn(1);
        Mockito.lenient().when(sender2.getNodeId()).thenReturn(2);
        Mockito.lenient().when(raftGroup.quorum()).thenReturn(2);
        Mockito.lenient().when(cluster.onSenderAvailable()).thenReturn(Flux.empty());
        Mockito.lenient().when(cluster.onSenderUnavailable()).thenReturn(Flux.empty());
    }

    @AfterEach
    void tearDown() {
        leaderRole.onExit(cluster, raftGroup, raftStorage);
    }

    @Test
    void leaderEmptyLog() throws InterruptedException {
        // given
        leaderRole = new LeaderRole(Duration.ZERO, Duration.ofSeconds(1));
        raftStorage.update(1, 0);
        given(raftGroup.availableSenders()).willReturn(Flux.just(sender1));
        given(sender1.appendEntries(eq(raftGroup), any(AppendEntriesRequest.class))).willReturn(appendEntriesResponseSuccess(1));

        // when
        leaderRole.onInit(cluster, raftGroup, raftStorage);
        Thread.sleep(100);

        // then
        ArgumentCaptor<AppendEntriesRequest> argument = ArgumentCaptor.forClass(AppendEntriesRequest.class);
        verify(sender1).appendEntries(eq(raftGroup), argument.capture());
        AppendEntriesRequest actualAppendEntriesRequest = argument.getValue();
        assertThat(actualAppendEntriesRequest.getTerm()).isEqualTo(1);
        assertThat(actualAppendEntriesRequest.getPrevLogIndex()).isEqualTo(0);
        assertThat(actualAppendEntriesRequest.getEntriesCount()).isEqualTo(0);
    }

    @Test
    void leaderAtStartupAssumesFollowerHasAllEntries() throws InterruptedException {
        // given
        leaderRole = new LeaderRole(Duration.ZERO, Duration.ofSeconds(1));
        raftStorage.update(1, 0);
        raftStorage.append(commandEntry(1,  "val1"));
        raftStorage.append(commandEntry(1,  "val2"));
        given(raftGroup.availableSenders()).willReturn(Flux.just(sender1));
        given(sender1.appendEntries(eq(raftGroup), any(AppendEntriesRequest.class))).willReturn(appendEntriesResponseSuccess(1));

        // when
        leaderRole.onInit(cluster, raftGroup, raftStorage);
        Thread.sleep(100);

        // then
        List<AppendEntriesRequest> appendEntriesHistory = appendEntriesHistory(times(1));
        assertAppendEntriesRequest(2, 0, appendEntriesHistory, 1);
    }

    @Test
    void leaderEmptyAppendEntry() throws InterruptedException {
        // given
        leaderRole = new LeaderRole(Duration.ZERO, Duration.ZERO);
        raftStorage.update(1, 0);
        given(raftGroup.availableSenders()).willReturn(Flux.just(sender1));
        given(sender1.appendEntries(eq(raftGroup), any(AppendEntriesRequest.class))).willReturn(appendEntriesResponseSuccess(1));
        leaderRole.onInit(cluster, raftGroup, raftStorage);

        // when
        raftStorage.append(commandEntry(1,  "val1"));
        leaderRole.markNewEntry(1L, System.currentTimeMillis());
        Thread.sleep(100);
        raftStorage.append(commandEntry(1,  "val2"));
        raftStorage.append(commandEntry(1,  "val3"));
        leaderRole.markNewEntry(3L, System.currentTimeMillis());
        Thread.sleep(100);

        // then
        List<AppendEntriesRequest> appendEntriesHistory = appendEntriesHistory(times(2));
        assertAppendEntriesRequest(0, 1, appendEntriesHistory, 1);
        assertAppendEntriesRequest(1, 2, appendEntriesHistory, 2);

        AppendEntriesRequest appendEntriesRequest = appendEntriesHistory.get(0);

        assertThat(appendEntriesRequest.getEntriesList().stream()
                .map(byteString -> deserialize(byteString.asReadOnlyByteBuffer(), CommandEntry.class))
                .map(CommandEntry::getValue)
                .map(String::new)
                .collect(Collectors.toList())).containsExactly("val1");

        appendEntriesRequest = appendEntriesHistory.get(1);

        assertThat(appendEntriesRequest.getEntriesList().stream()
                .map(byteString -> deserialize(byteString.asReadOnlyByteBuffer(), CommandEntry.class))
                .map(CommandEntry::getValue)
                .map(String::new)
                .collect(Collectors.toList())).containsExactly("val2", "val3");
    }

    @Test
    void leaderLogNotEmptyAppendEntry() throws InterruptedException {
        // given
        leaderRole = new LeaderRole(Duration.ZERO, Duration.ofSeconds(1));
        raftStorage.update(1, 0);
        raftStorage.append(commandEntry(1,  "val1"));
        raftStorage.append(commandEntry(1,  "val2"));
        given(raftGroup.availableSenders()).willReturn(Flux.just(sender1));
        given(sender1.appendEntries(eq(raftGroup), any(AppendEntriesRequest.class))).willReturn(appendEntriesResponseSuccess(1));
        leaderRole.onInit(cluster, raftGroup, raftStorage);

        // when
        Thread.sleep(50);
        raftStorage.append(commandEntry(1,  "val3"));
        leaderRole.markNewEntry(3L, System.currentTimeMillis());

        // then
        Thread.sleep(100);

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
    @Disabled
    void onAddServers() {
        // given
        given(raftGroup.getSenderById(7003)).willReturn(Mono.just(sender1));
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
        StepVerifier.create(leaderRole.onAddServer(cluster, raftGroup, raftStorage, addServerRequest()))
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
