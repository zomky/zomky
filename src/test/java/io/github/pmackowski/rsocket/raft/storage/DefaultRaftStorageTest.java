package io.github.pmackowski.rsocket.raft.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultRaftStorageTest {

    private static final int NODE_ID = 1;

    @TempDir
    Path folder;

    DefaultRaftStorage raftStorage, raftStorage2;

    @BeforeEach
    void before() {
        raftStorage = new DefaultRaftStorage(NODE_ID, folder.toAbsolutePath().toString());
    }

    @AfterEach
    void after() {
        raftStorage.close();
        if (raftStorage2 != null) {
            raftStorage2.close();
        }
    }

    @Test
    void nodeTermInitializedToZero() {
        int term = raftStorage.getTerm();
        assertThat(term).isEqualTo(0);
    }

    @Test
    void nodeVoteForInitializedToZero() {
        int votedFor = raftStorage.getVotedFor();
        assertThat(votedFor).isEqualTo(0);
    }

    @Test
    void updateNode() {
        raftStorage.update(2, 3);
        raftStorage.update(4, 5);

        assertThat(raftStorage.getTerm()).isEqualTo(4);
        assertThat(raftStorage.getVotedFor()).isEqualTo(5);
    }

    @Test
    void appendLog() {
        LogEntryInfo logEntryInfo1 = raftStorage.appendLog(1, ByteBuffer.wrap("Abc1".getBytes()));
        LogEntryInfo logEntryInfo2 = raftStorage.appendLog(1, ByteBuffer.wrap("Abc2".getBytes()));
        LogEntryInfo logEntryInfo3 = raftStorage.appendLog(2, ByteBuffer.wrap("Abc3".getBytes()));

        assertThat(logEntryInfo1).isEqualTo(new LogEntryInfo().index(1).term(1));
        assertThat(logEntryInfo2).isEqualTo(new LogEntryInfo().index(2).term(1));
        assertThat(logEntryInfo3).isEqualTo(new LogEntryInfo().index(3).term(2));

        assertThat(new String(raftStorage.getEntryByIndex(1).array())).isEqualTo("Abc1");
        assertThat(new String(raftStorage.getEntryByIndex(2).array())).isEqualTo("Abc2");
        assertThat(new String(raftStorage.getEntryByIndex(3).array())).isEqualTo("Abc3");
    }

    @Test
    void appendLogAfterInitialization() {
        raftStorage.appendLog(1, ByteBuffer.wrap("Abc1".getBytes()));
        raftStorage.appendLog(1, ByteBuffer.wrap("Abc2".getBytes()));

        raftStorage2 = new DefaultRaftStorage(NODE_ID, folder.toAbsolutePath().toString());
        raftStorage2.appendLog(2, ByteBuffer.wrap("Abc3".getBytes()));

        assertThat(raftStorage2.getTermByIndex(3)).isEqualTo(2);
    }

    // entries_size(n) # term1 # position1 # size1 #  ... # term(n) # position(n) # size(n) # entry1 # ... # entry(n)
    @Test
    void appendLogs() {
        ByteBuffer buffer = ByteBuffer.allocate(78);
        buffer.putInt(3);
        buffer.putInt(12 + 3 * 16);
        buffer.putInt(5 + 6 + 7);

        buffer.putInt(1);
        buffer.putLong(0);
        buffer.putInt(5);

        buffer.putInt(2);
        buffer.putLong(5);
        buffer.putInt(6);

        buffer.putInt(3);
        buffer.putLong(11);
        buffer.putInt(7);

        buffer.put("12345".getBytes());
        buffer.put("123456".getBytes());
        buffer.put("1234567".getBytes());


        raftStorage.appendLogs(buffer);

        assertThat(new String(raftStorage.getEntryByIndex(1).array())).isEqualTo("12345");
        assertThat(new String(raftStorage.getEntryByIndex(2).array())).isEqualTo("123456");
        assertThat(new String(raftStorage.getEntryByIndex(3).array())).isEqualTo("1234567");

        assertThat(raftStorage.getTermByIndex(1)).isEqualTo(1);
        assertThat(raftStorage.getTermByIndex(2)).isEqualTo(2);
        assertThat(raftStorage.getTermByIndex(3)).isEqualTo(3);
    }

    @Test
    void getLast() {
        raftStorage.appendLog(1, ByteBuffer.wrap("Abc1".getBytes()));
        LogEntryInfo logEntryInfo2 = raftStorage.appendLog(2, ByteBuffer.wrap("Abc2".getBytes()));

        assertThat(raftStorage.getLast()).isEqualTo(logEntryInfo2);
    }

    @Test
    void getLastAfterInitialization() {
        raftStorage.appendLog(1, ByteBuffer.wrap("Abc1".getBytes()));
        raftStorage.appendLog(2, ByteBuffer.wrap("Abc2".getBytes()));
        raftStorage.appendLog(2, ByteBuffer.wrap("Abc3".getBytes()));

        raftStorage2 = new DefaultRaftStorage(NODE_ID, folder.toAbsolutePath().toString());

        assertThat(raftStorage2.getLast()).isEqualTo(new LogEntryInfo().term(2).index(3));
    }

    @Test
    void getTermByIndex() {
        LogEntryInfo logEntryInfo = raftStorage.appendLog(2, ByteBuffer.wrap("Abc1".getBytes()));

        int term = raftStorage.getTermByIndex(logEntryInfo.getIndex());
        assertThat(term).isEqualTo(2);
    }

    @Test
    void getTermByIndexAfterInitialization() {
        LogEntryInfo logEntryInfo = raftStorage.appendLog(2, ByteBuffer.wrap("Abc1".getBytes()));

        raftStorage2 = new DefaultRaftStorage(NODE_ID, folder.toAbsolutePath().toString());

        int term = raftStorage2.getTermByIndex(logEntryInfo.getIndex());
        assertThat(term).isEqualTo(2);
    }

    @Test
    void getEntryByIndex() {
        LogEntryInfo logEntryInfo = raftStorage.appendLog(2, ByteBuffer.wrap("Abc1".getBytes()));
        ByteBuffer entry = raftStorage.getEntryByIndex(logEntryInfo.getIndex());

        assertThat(new String(entry.array())).isEqualTo("Abc1");
    }

    @Test
    @Disabled
    // Fails on Windows. See the reason https://bugs.java.com/bugdatabase/view_bug.do?bug_id=4715154
    void getEntries() {
        raftStorage.appendLog(1, ByteBuffer.wrap("Abc1".getBytes()));
        raftStorage.appendLog(2, ByteBuffer.wrap("Abc2 ".getBytes()));
        raftStorage.appendLog(3, ByteBuffer.wrap("Abc3  ".getBytes()));
        raftStorage.appendLog(4, ByteBuffer.wrap("Abc4   ".getBytes()));

        ByteBuffer actual = raftStorage.getEntriesByIndex(2,4);
        assertThat(actual.getInt()).isEqualTo(3); // number of entries
        assertThat(actual.getInt()).isEqualTo(60); // metadata size
        assertThat(actual.getInt()).isEqualTo(18); // content size

        // Abc2
        assertThat(actual.getInt()).isEqualTo(2); // term
        assertThat(actual.getLong()).isEqualTo(4); // position
        assertThat(actual.getInt()).isEqualTo(5); // size

        // Abc3
        assertThat(actual.getInt()).isEqualTo(3); // term
        assertThat(actual.getLong()).isEqualTo(9); // position
        assertThat(actual.getInt()).isEqualTo(6); // size

        // Abc4
        assertThat(actual.getInt()).isEqualTo(4); // term
        assertThat(actual.getLong()).isEqualTo(15); // position
        assertThat(actual.getInt()).isEqualTo(7); // size

        actual.position(60);

        byte[] content = new byte[18];
        actual.get(content, 0,18);
        assertThat(new String(content)).isEqualTo("Abc2 Abc3  Abc4   ");
    }

    @Test
    @Disabled
    // Fails on Windows. See the reason https://bugs.java.com/bugdatabase/view_bug.do?bug_id=4715154
    void getEntriesAndAppend() {
        raftStorage.appendLog(1, ByteBuffer.wrap("12345".getBytes()));
        raftStorage.appendLog(2, ByteBuffer.wrap("123456".getBytes()));
        raftStorage.appendLog(3, ByteBuffer.wrap("1234567".getBytes()));

        ByteBuffer actual = raftStorage.getEntriesByIndex(2,3);

        raftStorage.appendLogs(actual);
    }

    @Test
    void getEntryByIndexAfterInitialization() {
        LogEntryInfo logEntryInfo = raftStorage.appendLog(2, ByteBuffer.wrap("Abc1".getBytes()));

        raftStorage2 = new DefaultRaftStorage(NODE_ID, folder.toAbsolutePath().toString());
        ByteBuffer entry = raftStorage2.getEntryByIndex(logEntryInfo.getIndex());

        assertThat(new String(entry.array())).isEqualTo("Abc1");
    }

    @Test
    void truncateFromIndex() {
        raftStorage.appendLog(1, ByteBuffer.wrap("Abc1".getBytes()));
        raftStorage.appendLog(1, ByteBuffer.wrap("Abc2".getBytes()));
        raftStorage.appendLog(1, ByteBuffer.wrap("Abc3".getBytes()));
        raftStorage.appendLog(1, ByteBuffer.wrap("Abc3".getBytes()));

        raftStorage.truncateFromIndex(3);

        assertThat(raftStorage.getLast().getIndex()).isEqualTo(2);
    }

    @Test
    void truncateFromZeroIndex() {
        raftStorage.appendLog(1, ByteBuffer.wrap("Abc1".getBytes()));
        raftStorage.appendLog(1, ByteBuffer.wrap("Abc2".getBytes()));
        raftStorage.appendLog(1, ByteBuffer.wrap("Abc3".getBytes()));
        raftStorage.appendLog(1, ByteBuffer.wrap("Abc3".getBytes()));

        raftStorage.truncateFromIndex(0);

        assertThat(raftStorage.getLast().getIndex()).isEqualTo(0);
        assertThat(raftStorage.getLast().getTerm()).isEqualTo(0);
    }
}