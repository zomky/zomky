package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.rpc.AppendEntriesRequest;
import io.github.pmackowski.rsocket.raft.rpc.AppendEntriesResponse;
import io.github.pmackowski.rsocket.raft.storage.InMemoryRaftStorage;
import io.github.pmackowski.rsocket.raft.storage.RaftStorage;
import io.github.pmackowski.rsocket.raft.storage.log.entry.CommandEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.retry.Repeat;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import static io.github.pmackowski.rsocket.raft.storage.log.serializer.LogEntrySerializer.deserialize;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class LeaderRoleTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderRoleTest.class);

    private static final Repeat<Object> NO_REPEAT = Repeat.onlyIf(objectRepeatContext -> false);

    LeaderRole leaderRole;

    @Mock
    Sender sender1, sender2;

    @Mock
    DefaultRaftServer node;

    RaftStorage raftStorage = new InMemoryRaftStorage();

    @BeforeEach
    void setUp() {
        Mockito.lenient().when(sender1.getNodeId()).thenReturn(1);
        Mockito.lenient().when(sender2.getNodeId()).thenReturn(2);
        Mockito.lenient().when(node.quorum()).thenReturn(2);
    }

    @Test
    void leaderEmptyLog() {
        // given
        leaderRole = new LeaderRole(Repeat.times(0));
        raftStorage.update(1, 0);
        given(node.availableSenders()).willReturn(Flux.just(sender1));
        given(sender1.appendEntries(any(AppendEntriesRequest.class))).willReturn(appendEntriesResponseSuccess(1));

        // when
        leaderRole.onInit(node, raftStorage);

        // then
        ArgumentCaptor<AppendEntriesRequest> argument = ArgumentCaptor.forClass(AppendEntriesRequest.class);
        verify(sender1).appendEntries(argument.capture());
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
        given(sender1.appendEntries(any(AppendEntriesRequest.class))).willReturn(appendEntriesResponseSuccess(1));

        // when
        leaderRole.onInit(node, raftStorage);

        // then
        ArgumentCaptor<AppendEntriesRequest> argument = ArgumentCaptor.forClass(AppendEntriesRequest.class);
        verify(sender1).appendEntries(argument.capture());
        AppendEntriesRequest actualAppendEntriesRequest = argument.getValue();
        assertThat(actualAppendEntriesRequest.getTerm()).isEqualTo(1);
        assertThat(actualAppendEntriesRequest.getPrevLogIndex()).isEqualTo(2);
        assertThat(actualAppendEntriesRequest.getEntriesCount()).isEqualTo(0);
    }

    @Test
    void leaderEmptyAppendEntry() throws InterruptedException {
        // given
        leaderRole = new LeaderRole(Repeat.once().fixedBackoff(Duration.ofMillis(20)));
        raftStorage.update(1, 0);
        given(node.availableSenders()).willReturn(Flux.just(sender1));
        given(sender1.appendEntries(any(AppendEntriesRequest.class))).willReturn(appendEntriesResponseSuccess(1));
        leaderRole.onInit(node, raftStorage);

        // when
        raftStorage.append(commandEntry(1,  "val1"));
        raftStorage.append(commandEntry(1,  "val2"));

        // then
        Thread.sleep(50);

        ArgumentCaptor<AppendEntriesRequest> argument = ArgumentCaptor.forClass(AppendEntriesRequest.class);
        verify(sender1, times(2)).appendEntries(argument.capture());

        List<AppendEntriesRequest> appendEntries = argument.getAllValues();

        AppendEntriesRequest appendEntriesRequest = appendEntries.get(0);
        assertThat(appendEntriesRequest.getTerm()).isEqualTo(1);
        assertThat(appendEntriesRequest.getPrevLogIndex()).isEqualTo(0);
        assertThat(appendEntriesRequest.getEntriesCount()).isEqualTo(0);

        appendEntriesRequest = appendEntries.get(1);
        assertThat(appendEntriesRequest.getTerm()).isEqualTo(1);
        assertThat(appendEntriesRequest.getPrevLogIndex()).isEqualTo(0);
        assertThat(appendEntriesRequest.getEntriesCount()).isEqualTo(2);

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
        given(sender1.appendEntries(any(AppendEntriesRequest.class))).willReturn(appendEntriesResponseSuccess(1));
        leaderRole.onInit(node, raftStorage);

        // when
        raftStorage.append(commandEntry(1,  "val3"));

        // then
        Thread.sleep(120);

        ArgumentCaptor<AppendEntriesRequest> argument = ArgumentCaptor.forClass(AppendEntriesRequest.class);
        verify(sender1, times(2)).appendEntries(argument.capture());

        List<AppendEntriesRequest> appendEntries = argument.getAllValues();

        AppendEntriesRequest appendEntriesRequest = appendEntries.get(0);
        assertThat(appendEntriesRequest.getTerm()).isEqualTo(1);
        assertThat(appendEntriesRequest.getPrevLogIndex()).isEqualTo(2);
        assertThat(appendEntriesRequest.getEntriesCount()).isEqualTo(0);

        appendEntriesRequest = appendEntries.get(1);
        assertThat(appendEntriesRequest.getTerm()).isEqualTo(1);
        assertThat(appendEntriesRequest.getPrevLogIndex()).isEqualTo(2);
        assertThat(appendEntriesRequest.getEntriesCount()).isEqualTo(1);

        assertThat(appendEntriesRequest.getEntriesList().stream()
                .map(byteString -> deserialize(byteString.asReadOnlyByteBuffer(), CommandEntry.class))
                .map(CommandEntry::getValue)
                .map(String::new)
                .collect(Collectors.toList())).containsExactly("val3");
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
