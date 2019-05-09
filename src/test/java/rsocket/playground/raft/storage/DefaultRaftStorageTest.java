package rsocket.playground.raft.storage;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

public class DefaultRaftStorageTest {

    private static final int NODE_ID = 1;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryFolder folder2 = new TemporaryFolder();

    private DefaultRaftStorage zomkyStorage;

    @Before
    public void setUp() {
        zomkyStorage = new DefaultRaftStorage(NODE_ID, folder.getRoot().getAbsolutePath());
    }

    @Test
    public void nodeTermInitializedToZero() {
        int term = zomkyStorage.getTerm();
        assertThat(term).isEqualTo(0);
    }

    @Test
    public void nodeVoteForInitializedToZero() {
        int votedFor = zomkyStorage.getVotedFor();
        assertThat(votedFor).isEqualTo(0);
    }

    @Test
    public void updateNode() {
        zomkyStorage.update(2, 3);
        zomkyStorage.update(4, 5);

        assertThat(zomkyStorage.getTerm()).isEqualTo(4);
        assertThat(zomkyStorage.getVotedFor()).isEqualTo(5);
    }

    @Test
    public void appendLog() {
        LogEntryInfo logEntryInfo1 = zomkyStorage.appendLog(1, ByteBuffer.wrap("Abc1".getBytes()));
        LogEntryInfo logEntryInfo2 = zomkyStorage.appendLog(1, ByteBuffer.wrap("Abc2".getBytes()));
        LogEntryInfo logEntryInfo3 = zomkyStorage.appendLog(2, ByteBuffer.wrap("Abc3".getBytes()));

        assertThat(logEntryInfo1).isEqualTo(new LogEntryInfo().index(1).term(1));
        assertThat(logEntryInfo2).isEqualTo(new LogEntryInfo().index(2).term(1));
        assertThat(logEntryInfo3).isEqualTo(new LogEntryInfo().index(3).term(2));

        assertThat(new String(zomkyStorage.getEntryByIndex(1).array())).isEqualTo("Abc1");
        assertThat(new String(zomkyStorage.getEntryByIndex(2).array())).isEqualTo("Abc2");
        assertThat(new String(zomkyStorage.getEntryByIndex(3).array())).isEqualTo("Abc3");
    }

    @Test
    public void appendLogAfterInitialization() {
        zomkyStorage.appendLog(1, ByteBuffer.wrap("Abc1".getBytes()));
        zomkyStorage.appendLog(1, ByteBuffer.wrap("Abc2".getBytes()));

        RaftStorage raftStorage2 = new DefaultRaftStorage(NODE_ID, folder.getRoot().getAbsolutePath());
        raftStorage2.appendLog(2, ByteBuffer.wrap("Abc3".getBytes()));

        assertThat(raftStorage2.getTermByIndex(3)).isEqualTo(2);
    }

    // entries_size(n) # term1 # position1 # size1 #  ... # term(n) # position(n) # size(n) # entry1 # ... # entry(n)
    @Test
    public void appendLogs() {
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


        zomkyStorage.appendLogs(buffer);

        assertThat(new String(zomkyStorage.getEntryByIndex(1).array())).isEqualTo("12345");
        assertThat(new String(zomkyStorage.getEntryByIndex(2).array())).isEqualTo("123456");
        assertThat(new String(zomkyStorage.getEntryByIndex(3).array())).isEqualTo("1234567");

        assertThat(zomkyStorage.getTermByIndex(1)).isEqualTo(1);
        assertThat(zomkyStorage.getTermByIndex(2)).isEqualTo(2);
        assertThat(zomkyStorage.getTermByIndex(3)).isEqualTo(3);
    }

    @Test
    public void getLast() {
        zomkyStorage.appendLog(1, ByteBuffer.wrap("Abc1".getBytes()));
        LogEntryInfo logEntryInfo2 = zomkyStorage.appendLog(2, ByteBuffer.wrap("Abc2".getBytes()));

        assertThat(zomkyStorage.getLast()).isEqualTo(logEntryInfo2);
    }

    @Test
    public void getLastAfterInitialization() {
        zomkyStorage.appendLog(1, ByteBuffer.wrap("Abc1".getBytes()));
        zomkyStorage.appendLog(2, ByteBuffer.wrap("Abc2".getBytes()));
        zomkyStorage.appendLog(2, ByteBuffer.wrap("Abc3".getBytes()));

        RaftStorage raftStorage2 = new DefaultRaftStorage(NODE_ID, folder.getRoot().getAbsolutePath());

        assertThat(raftStorage2.getLast()).isEqualTo(new LogEntryInfo().term(2).index(3));
    }

    @Test
    public void getTermByIndex() {
        LogEntryInfo logEntryInfo = zomkyStorage.appendLog(2, ByteBuffer.wrap("Abc1".getBytes()));

        int term = zomkyStorage.getTermByIndex(logEntryInfo.getIndex());
        assertThat(term).isEqualTo(2);
    }

    @Test
    public void getTermByIndexAfterInitialization() {
        LogEntryInfo logEntryInfo = zomkyStorage.appendLog(2, ByteBuffer.wrap("Abc1".getBytes()));

        RaftStorage raftStorage2 = new DefaultRaftStorage(NODE_ID, folder.getRoot().getAbsolutePath());

        int term = raftStorage2.getTermByIndex(logEntryInfo.getIndex());
        assertThat(term).isEqualTo(2);
    }

    @Test
    public void getEntryByIndex() {
        LogEntryInfo logEntryInfo = zomkyStorage.appendLog(2, ByteBuffer.wrap("Abc1".getBytes()));
        ByteBuffer entry = zomkyStorage.getEntryByIndex(logEntryInfo.getIndex());

        assertThat(new String(entry.array())).isEqualTo("Abc1");
    }

    @Test
    public void getEntries() {
        zomkyStorage.appendLog(1, ByteBuffer.wrap("Abc1".getBytes()));
        zomkyStorage.appendLog(2, ByteBuffer.wrap("Abc2 ".getBytes()));
        zomkyStorage.appendLog(3, ByteBuffer.wrap("Abc3  ".getBytes()));
        zomkyStorage.appendLog(4, ByteBuffer.wrap("Abc4   ".getBytes()));

        ByteBuffer actual = zomkyStorage.getEntriesByIndex(2,4);
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
    public void getEntriesAndAppend() {
        zomkyStorage.appendLog(1, ByteBuffer.wrap("12345".getBytes()));
        zomkyStorage.appendLog(2, ByteBuffer.wrap("123456".getBytes()));
        zomkyStorage.appendLog(3, ByteBuffer.wrap("1234567".getBytes()));

        ByteBuffer actual = zomkyStorage.getEntriesByIndex(2,3);

        zomkyStorage.appendLogs(actual);

    }

    @Test
    public void getEntryByIndexAfterInitialization() {
        LogEntryInfo logEntryInfo = zomkyStorage.appendLog(2, ByteBuffer.wrap("Abc1".getBytes()));

        RaftStorage raftStorage2 = new DefaultRaftStorage(NODE_ID, folder.getRoot().getAbsolutePath());
        ByteBuffer entry = raftStorage2.getEntryByIndex(logEntryInfo.getIndex());

        assertThat(new String(entry.array())).isEqualTo("Abc1");
    }

    @Test
    public void truncateFromIndex() {
        zomkyStorage.appendLog(1, ByteBuffer.wrap("Abc1".getBytes()));
        zomkyStorage.appendLog(1, ByteBuffer.wrap("Abc2".getBytes()));
        zomkyStorage.appendLog(1, ByteBuffer.wrap("Abc3".getBytes()));
        zomkyStorage.appendLog(1, ByteBuffer.wrap("Abc3".getBytes()));

        zomkyStorage.truncateFromIndex(3);

        assertThat(zomkyStorage.getLast().getIndex()).isEqualTo(2);
    }

    @Test
    public void truncateFromZeroIndex() {
        zomkyStorage.appendLog(1, ByteBuffer.wrap("Abc1".getBytes()));
        zomkyStorage.appendLog(1, ByteBuffer.wrap("Abc2".getBytes()));
        zomkyStorage.appendLog(1, ByteBuffer.wrap("Abc3".getBytes()));
        zomkyStorage.appendLog(1, ByteBuffer.wrap("Abc3".getBytes()));

        zomkyStorage.truncateFromIndex(0);

        assertThat(zomkyStorage.getLast().getIndex()).isEqualTo(0);
        assertThat(zomkyStorage.getLast().getTerm()).isEqualTo(0);
    }
}