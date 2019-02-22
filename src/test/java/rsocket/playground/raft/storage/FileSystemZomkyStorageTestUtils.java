package rsocket.playground.raft.storage;

import java.nio.ByteBuffer;

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

}
