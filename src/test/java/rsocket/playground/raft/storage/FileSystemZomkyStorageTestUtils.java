package rsocket.playground.raft.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;

import static rsocket.playground.raft.storage.FileSystemZomkyStorage.ZOMKY_DIRECTORY;
import static rsocket.playground.raft.storage.FileSystemZomkyStorage.ZOMKY_LOG_ENTRIES_CONTENT;

public class FileSystemZomkyStorageTestUtils {

    public static String getContent(ZomkyStorage zomkyStorage, long expectedEntries) {
        ByteBuffer entries = zomkyStorage.getEntriesByIndex(1, expectedEntries);
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
