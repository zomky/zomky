package io.github.zomky.raft;

import io.github.zomky.storage.RaftStorage;
import io.github.zomky.storage.log.entry.IndexedLogEntry;
import io.github.zomky.storage.log.entry.LogEntry;
import io.github.zomky.storage.log.reader.LogStorageReader;
import io.github.zomky.storage.meta.Configuration;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class CommitIndexUpdaterTest {

    @Mock
    RaftStorage raftStorage;

    @Mock
    RaftGroup raftGroup;

    @Test
    void updateSameTerm() {
        // given
        given(raftStorage.getTerm()).willReturn(1);
        given(raftStorage.getTermByIndex(anyLong())).willReturn(1);
        given(raftGroup.quorum()).willReturn(2);
        given(raftGroup.getCommitIndex()).willReturn(3L);

        Map<Integer, Long> matchIndex = new HashMap<>();
        matchIndex.put(7001, 5L);
        matchIndex.put(7002, 4L);

        CommitIndexUpdater commitIndexUpdater = new CommitIndexUpdater(matchIndex);

        // when
        commitIndexUpdater.update(raftStorage, raftGroup, 7001, 6L);

        // then
        verify(raftGroup).setCommitIndex(6L);
    }

    @Test
    void calculateGreaterTerm() {
        // given
        given(raftStorage.getTerm()).willReturn(4);
        given(raftStorage.getTermByIndex(anyLong())).willAnswer(arg -> {
            final long index = arg.getArgument(0);
            return (index >= 4) ? 3 : 2;
        });
        given(raftGroup.quorum()).willReturn(2);
        given(raftGroup.getCommitIndex()).willReturn(1L);

        Map<Integer, Long> matchIndex = new HashMap<>();
        matchIndex.put(7001, 5L);
        matchIndex.put(7002, 4L);

        CommitIndexUpdater commitIndexUpdater = new CommitIndexUpdater(matchIndex);

        // when
        commitIndexUpdater.update(raftStorage, raftGroup, 7001, 6L);

        // then
        verify(raftGroup, never()).setCommitIndex(anyLong());
    }

}