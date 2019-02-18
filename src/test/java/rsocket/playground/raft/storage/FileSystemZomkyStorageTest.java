package rsocket.playground.raft.storage;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class FileSystemZomkyStorageTest {

    private static final int NODE_ID = 1;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private FileSystemZomkyStorage zomkyStorage;

    @Before
    public void setUp() {
        zomkyStorage = new FileSystemZomkyStorage(NODE_ID, folder.getRoot().getAbsolutePath());
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
    }

    @Test
    public void appendLogAfterInitialization() {
        zomkyStorage.appendLog(1, ByteBuffer.wrap("Abc1".getBytes()));
        zomkyStorage.appendLog(1, ByteBuffer.wrap("Abc2".getBytes()));

        ZomkyStorage zomkyStorage2 = new FileSystemZomkyStorage(NODE_ID, folder.getRoot().getAbsolutePath());
        zomkyStorage2.appendLog(2, ByteBuffer.wrap("Abc3".getBytes()));

        assertThat(zomkyStorage2.getTermByIndex(3)).isEqualTo(2);
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

        ZomkyStorage zomkyStorage2 = new FileSystemZomkyStorage(NODE_ID, folder.getRoot().getAbsolutePath());

        assertThat(zomkyStorage2.getLast()).isEqualTo(new LogEntryInfo().term(2).index(3));
    }

    @Test
    public void getTermByIndexOverflow() {
        assertThatThrownBy(() -> zomkyStorage.getTermByIndex(1))
                .isInstanceOf(ZomkyStorageException.class)
                .hasCauseInstanceOf(BufferUnderflowException.class);
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

        ZomkyStorage zomkyStorage2 = new FileSystemZomkyStorage(NODE_ID, folder.getRoot().getAbsolutePath());

        int term = zomkyStorage2.getTermByIndex(logEntryInfo.getIndex());
        assertThat(term).isEqualTo(2);
    }

    @Test
    public void getEntryByIndexOverflow() {
        assertThatThrownBy(() -> zomkyStorage.getEntryByIndex(1))
                .isInstanceOf(ZomkyStorageException.class)
                .hasCauseInstanceOf(BufferUnderflowException.class);
    }

    @Test
    public void getEntryByIndex() {
        LogEntryInfo logEntryInfo = zomkyStorage.appendLog(2, ByteBuffer.wrap("Abc1".getBytes()));
        ByteBuffer entry = zomkyStorage.getEntryByIndex(logEntryInfo.getIndex());

        assertThat(new String(entry.array())).isEqualTo("Abc1");
    }

    @Test
    public void getEntryByIndexAfterInitialization() {
        LogEntryInfo logEntryInfo = zomkyStorage.appendLog(2, ByteBuffer.wrap("Abc1".getBytes()));

        ZomkyStorage zomkyStorage2 = new FileSystemZomkyStorage(NODE_ID, folder.getRoot().getAbsolutePath());
        ByteBuffer entry = zomkyStorage2.getEntryByIndex(logEntryInfo.getIndex());

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