package io.github.zomky.raft;

import io.github.zomky.storage.RaftStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
class CommitIndexCalculatorTest {
    @Mock
    RaftStorage raftStorage;

    @Mock
    RaftGroup raftGroup;

    CommitIndexCalculator commitIndexCalculator = new CommitIndexCalculator();

    @Test
    void calculate() {
        // given
        given(raftStorage.getTerm()).willReturn(1);
        given(raftStorage.getTermByIndex(anyLong())).willReturn(1);
        given(raftGroup.quorum()).willReturn(2);

        Map<Integer, Long> matchIndex = new HashMap<>();
        long lastIndex = 6;
        matchIndex.put(7001, 5L);
        matchIndex.put(7002, 4L);

        // when
        long actual = commitIndexCalculator.calculate(raftStorage, raftGroup, matchIndex, lastIndex);

        // then
        assertThat(actual).isEqualTo(5);
    }

    @Test
    void calculateTermsDiffer() {
        // given
        given(raftStorage.getTerm()).willReturn(2);
        given(raftStorage.getTermByIndex(anyLong())).willAnswer(arg -> {
            final long index = arg.getArgument(0);
            return (index >= 4) ? 3 : 2;
        });
        given(raftGroup.quorum()).willReturn(2);

        Map<Integer, Long> matchIndex = new HashMap<>();
        long lastIndex = 6;
        matchIndex.put(7001, 5L);
        matchIndex.put(7002, 4L);

        // when
        long actual = commitIndexCalculator.calculate(raftStorage, raftGroup, matchIndex, lastIndex);

        // then
        assertThat(actual).isEqualTo(3);
    }
}