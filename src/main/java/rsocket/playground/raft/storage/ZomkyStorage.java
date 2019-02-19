package rsocket.playground.raft.storage;

import java.nio.ByteBuffer;

public interface ZomkyStorage {

    int getTerm();

    int getVotedFor();

    /**
     * @param term
     * @param votedFor (voteFor == 0) -> not voted in term
     */
    void update(int term, int votedFor);

    LogEntryInfo appendLog(int term, ByteBuffer buffer);

    LogEntryInfo appendLogs(ByteBuffer buffer);

    int getTermByIndex(long index);

    ByteBuffer getEntriesByIndex(long indexFrom, long indexTo);

    ByteBuffer getEntryByIndex(long index);

    LogEntryInfo getLast();

    void truncateFromIndex(long index);

    void close();

}
