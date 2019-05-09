package rsocket.playground.raft.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;

import static rsocket.playground.raft.storage.DefaultRaftStorage.ZOMKY_DIRECTORY;
import static rsocket.playground.raft.storage.DefaultRaftStorage.ZOMKY_LOG_ENTRIES_CONTENT;

public class DefaultRaftStorageTestUtils {

    public static String getContent(RaftStorage raftStorage, long expectedEntries) {
        ByteBuffer entries = raftStorage.getEntriesByIndex(1, expectedEntries);
        int nbEntries = entries.getInt();
        int metadata = entries.getInt();
        int contentSize = entries.getInt();

        entries.position(metadata);
        byte[] content = new byte[contentSize];
        entries.get(content, 0,contentSize);
        return new String(content);
    }

    public static String getContent(String directory, int nodeId) throws IOException {
        return new String(Files.readAllBytes(Paths.get(directory, ZOMKY_DIRECTORY, String.format(ZOMKY_LOG_ENTRIES_CONTENT, nodeId))));
    }

}
