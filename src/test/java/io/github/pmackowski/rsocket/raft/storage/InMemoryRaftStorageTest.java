package io.github.pmackowski.rsocket.raft.storage;

import io.github.pmackowski.rsocket.raft.storage.log.entry.CommandEntry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryRaftStorageTest {

    private InMemoryRaftStorage raftStorage = new InMemoryRaftStorage();

    @Test
    void storageAfterInitialization() {
        assertThat(raftStorage.getVotedFor()).isEqualTo(0);
        assertThat(raftStorage.getTerm()).isEqualTo(0);
        assertThat(raftStorage.getLastIndexedTerm().getIndex()).isEqualTo(0);
        assertThat(raftStorage.getTermByIndex(0)).isEqualTo(0);
    }

    @Test
    void updateTermAndVotedFor() {
        raftStorage.update(1, 7001);

        assertThat(raftStorage.getVotedFor()).isEqualTo(7001);
        assertThat(raftStorage.getTerm()).isEqualTo(1);
        assertThat(raftStorage.getLastIndexedTerm().getIndex()).isEqualTo(0);
        assertThat(raftStorage.getTermByIndex(0)).isEqualTo(0);
    }

    @Test
    void append() {
        final CommandEntry c1 = commandEntry(1, "val1");
        final CommandEntry c2 = commandEntry(1, "val2");
        final CommandEntry c3 = commandEntry(1, "val3");

        raftStorage.append(c1);
        raftStorage.append(c2);
        raftStorage.append(c3);

        assertThat(raftStorage.getLastIndexedTerm().getIndex()).isEqualTo(3);
        assertThat(raftStorage.getEntryByIndex(1).get().getLogEntry()).isEqualTo(c1);
        assertThat(raftStorage.getEntryByIndex(2).get().getLogEntry()).isEqualTo(c2);
        assertThat(raftStorage.getEntryByIndex(3).get().getLogEntry()).isEqualTo(c3);
    }

    @Test
    void truncate() {
        raftStorage.append(commandEntry(1, "val1"));
        raftStorage.append(commandEntry(1, "val2"));
        raftStorage.append(commandEntry(1, "val3"));

        raftStorage.truncateFromIndex(2);

        assertThat(raftStorage.getLastIndexedTerm().getIndex()).isEqualTo(1);
        assertThat(raftStorage.getEntryByIndex(2)).isEmpty();
    }

    private CommandEntry commandEntry(int term, String value) {
        return new CommandEntry(term, System.currentTimeMillis(), value.getBytes());
    }

}