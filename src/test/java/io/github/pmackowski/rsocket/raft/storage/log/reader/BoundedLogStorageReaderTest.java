package io.github.pmackowski.rsocket.raft.storage.log.reader;

import io.github.pmackowski.rsocket.raft.storage.log.entry.IndexedLogEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BoundedLogStorageReaderTest {

    BoundedLogStorageReader boundedLogStorageReader;

    @Mock
    LogStorageReader logStorageReader;

    @Mock
    IndexedLogEntry logEntry1, logEntry2, logEntry3;

    @BeforeEach
    void setUp() {
        boundedLogStorageReader = new BoundedLogStorageReader(logStorageReader);
    }

    @Test
    void getEntriesByIndex() {
        when(logStorageReader.hasNext()).thenReturn(true);
        when(logStorageReader.next()).thenReturn(logEntry1, logEntry2, logEntry3);

        List<IndexedLogEntry> actual = boundedLogStorageReader.getEntriesByIndex(1, 2);

        assertThat(actual).containsExactly(logEntry1, logEntry2);
    }
}