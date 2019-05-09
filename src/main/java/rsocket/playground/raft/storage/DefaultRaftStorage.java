package rsocket.playground.raft.storage;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Default implementation (so far not optimal implementation)
 * TODO remove synchronized
 */
public class DefaultRaftStorage implements RaftStorage {

    private static final int INDEX_TERM_FILE_ENTRY_SIZE = Long.BYTES + 2 * Integer.BYTES;

    public static final String ZOMKY_DIRECTORY = ".zomky";
    private static final String ZOMKY_NODE_DATA = "node.%s.zomky";
    private static final String ZOMKY_LOG_ENTRIES_METADATA = "node.%s.entries.metadata.zomky";
    public static final String ZOMKY_LOG_ENTRIES_CONTENT = "node.%s.entries.content.zomky";

    private Path directory;

    private RandomAccessFile nodeDataFile;
    private FileChannel nodeDataFileChannel;

    private RandomAccessFile metadataFile;
    private FileChannel metadataFileChannel;
    private FileChannel metadataFileAppendLogChannel;
    private RandomAccessFile contentFile;
    private RandomAccessFile contentFileAppendLog;
    private FileChannel contentFileChannel;
    private FileChannel contentFileAppendLogChannel;
    private ByteBuffer byteBuffer = ByteBuffer.allocate(INDEX_TERM_FILE_ENTRY_SIZE);

    private AtomicLong lastIndex;
    private AtomicInteger lastTerm;

    public DefaultRaftStorage(int nodeId) {
        this(nodeId, System.getProperty("user.home"));
    }

    public DefaultRaftStorage(int nodeId, String directory) {
        this.directory = Paths.get(directory, ZOMKY_DIRECTORY);
        try {
            if (Files.notExists(this.directory)) {
                Files.createDirectory(this.directory);
            }
        } catch (IOException e) {
            throw new StorageException(e);
        }

        try {
            nodeDataFile = new RandomAccessFile(filePath(ZOMKY_NODE_DATA, nodeId), "rw");
            nodeDataFileChannel = nodeDataFile.getChannel();
            metadataFile = new RandomAccessFile(filePath(ZOMKY_LOG_ENTRIES_METADATA, nodeId), "rw");
            metadataFileChannel = new RandomAccessFile(filePath(ZOMKY_LOG_ENTRIES_METADATA, nodeId), "r").getChannel();
            metadataFileAppendLogChannel = metadataFile.getChannel();
            contentFileAppendLog = new RandomAccessFile(filePath(ZOMKY_LOG_ENTRIES_CONTENT, nodeId), "rw");
            contentFile = new RandomAccessFile(filePath(ZOMKY_LOG_ENTRIES_CONTENT, nodeId), "r");

            contentFileChannel = contentFile.getChannel();
            contentFileAppendLogChannel = contentFileAppendLog.getChannel();

            if (nodeDataFileChannel.size() == 0) {
                update(0, 0);
            }

            lastIndex = new AtomicLong(metadataFileChannel.size() / INDEX_TERM_FILE_ENTRY_SIZE);
            metadataFileAppendLogChannel.position(lastIndex.get() * INDEX_TERM_FILE_ENTRY_SIZE);
            if (lastIndex.get() > 0) {
                lastTerm = new AtomicInteger(getTermByIndex(lastIndex.get()));
            } else {
                lastTerm = new AtomicInteger(0);
            }

            contentFileAppendLogChannel.position(contentFileAppendLogChannel.size());
        } catch (Exception e) {
            close();
            throw new StorageException(e);
        }
    }

    @Override
    public synchronized int getTerm() {
        try {
            nodeDataFileChannel.position(0);
            ByteBuffer metadataBuffer = ByteBuffer.allocate(Integer.BYTES);
            nodeDataFileChannel.read(metadataBuffer);
            metadataBuffer.flip();
            return metadataBuffer.getInt();
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public synchronized int getVotedFor() {
        try {
            nodeDataFileChannel.position(Integer.BYTES);
            ByteBuffer metadataBuffer = ByteBuffer.allocate(Integer.BYTES);
            nodeDataFileChannel.read(metadataBuffer);
            metadataBuffer.flip();
            return metadataBuffer.getInt();
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public synchronized void update(int term, int votedFor) {
        try {
            nodeDataFileChannel.position(0);
            ByteBuffer byteBuffer = ByteBuffer.allocate(2 * Integer.BYTES);
            byteBuffer.putInt(term);
            byteBuffer.putInt(votedFor);
            byteBuffer.flip();
            nodeDataFileChannel.write(byteBuffer);
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public synchronized LogEntryInfo appendLog(int term, ByteBuffer buffer) {
        try {
            int messageSize = buffer.remaining();
            contentFileAppendLogChannel.write(buffer);
            long position = contentFileAppendLogChannel.size() - messageSize;

            byteBuffer.position(0);
            byteBuffer.putInt(term);
            byteBuffer.putLong(position);
            byteBuffer.putInt(messageSize);
            byteBuffer.flip();
            metadataFileAppendLogChannel.write(byteBuffer);
            lastTerm.set(term);
            long lastIdx = lastIndex.incrementAndGet();
            return new LogEntryInfo()
                    .index(lastIdx)
                    .term(term);
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    // entries_size(n) # metadata_size # content_size # term1 # position1 # size1 #  ... # term(n) # position(n) # size(n) # entry1 # ... # entry(n)
    @Override
    public synchronized LogEntryInfo appendLogs(ByteBuffer buffer) {
        try {
            System.out.println("APPEND LOGS STARTED");
            buffer.position(0);
            int numberOfEntries = buffer.getInt();
            int metadataSize = buffer.getInt();
            int contentSize = buffer.getInt();

            ByteBuffer metadataBuffer = ByteBuffer.wrap(buffer.array(), 12    , metadataSize - 12);
            metadataFileAppendLogChannel.write(metadataBuffer);

            metadataBuffer.position(12);
            for (int i=0; i < numberOfEntries; i++) {
                System.out.println(String.format("%s, %s, %s", metadataBuffer.getInt(), metadataBuffer.getLong(), metadataBuffer.getInt()));
            }

            ByteBuffer contentBuffer = ByteBuffer.wrap(buffer.array(), metadataSize, contentSize);
            contentFileAppendLogChannel.write(contentBuffer);
            lastIndex = new AtomicLong(metadataFileChannel.size() / INDEX_TERM_FILE_ENTRY_SIZE);
            lastTerm = new AtomicInteger(getTermByIndex(lastIndex.get()));
            System.out.println("APPEND LOGS FINISHED");
        } catch (IOException e) {
            throw new StorageException(e);
        }
        return new LogEntryInfo()
                .index(lastIndex.get())
                .term(lastTerm.get());
    }

    @Override
    public synchronized int getTermByIndex(long index) {
        try {
            if (index == 0 || (metadataFileChannel.size() / INDEX_TERM_FILE_ENTRY_SIZE < index)) {
                return 0;
            }
            metadataFileChannel.position((index-1) * INDEX_TERM_FILE_ENTRY_SIZE);
            ByteBuffer metadataBuffer = ByteBuffer.allocate(Integer.BYTES);
            metadataFileChannel.read(metadataBuffer);
            metadataBuffer.flip();
            return metadataBuffer.getInt();
        } catch (IOException | BufferUnderflowException e) {
            throw new StorageException("index " + index, e);
        }
    }

    @Override
    public synchronized ByteBuffer getEntryByIndex(long index) {
        try {
            if (index == 0 || (metadataFileChannel.size() / INDEX_TERM_FILE_ENTRY_SIZE < index)) {
                return null;
            }
            metadataFileChannel.position((index-1) * INDEX_TERM_FILE_ENTRY_SIZE + Integer.BYTES);
            ByteBuffer metadataBuffer = ByteBuffer.allocate(INDEX_TERM_FILE_ENTRY_SIZE - Integer.BYTES);
            metadataFileChannel.read(metadataBuffer);
            metadataBuffer.flip();
            long entryPosition = metadataBuffer.getLong();
            int entrySize = metadataBuffer.getInt();
            ByteBuffer contentBuffer = ByteBuffer.allocate(entrySize);
            contentFileChannel.position(entryPosition);
            contentFileChannel.read(contentBuffer);

            return contentBuffer;
        } catch (IOException | BufferUnderflowException e) {
            throw new StorageException("index " + index, e);
        }
    }

    // entries_size(n) # term1 # position1 # size1 #  ... # term(n) # position(n) # size(n) # entry1 # ... # entry(n)
    @Override
    public synchronized ByteBuffer getEntriesByIndex(long indexFrom, long indexTo) {
        try {
            int numberOfEntries = (int) (indexTo - indexFrom + 1);
            int metadataSize = numberOfEntries * INDEX_TERM_FILE_ENTRY_SIZE;
            MappedByteBuffer map = metadataFileChannel.map(
                    FileChannel.MapMode.READ_ONLY,
                    (indexFrom - 1) * INDEX_TERM_FILE_ENTRY_SIZE,
                    metadataSize);

            long startPosition = map.getLong(Integer.BYTES);
            long endPosition = map.getLong(metadataSize - Integer.BYTES - Long.BYTES) +
                               map.getInt(metadataSize - Integer.BYTES);

            int metadataSize2 = 3*Integer.BYTES + numberOfEntries*INDEX_TERM_FILE_ENTRY_SIZE;
            int contentSize = (int) (endPosition - startPosition);
            int allSize = metadataSize2 + contentSize;
            ByteBuffer contentBuffer = ByteBuffer.allocate(allSize);
            contentBuffer.putInt(numberOfEntries);
            contentBuffer.putInt(metadataSize2);
            contentBuffer.putInt(contentSize);

            long i = indexFrom;
            while (i <= indexTo) {
                contentBuffer.putInt(map.getInt());
                contentBuffer.putLong(map.getLong());
                contentBuffer.putInt(map.getInt());
                i++;
            }

            contentFileChannel.position(startPosition);
            contentFileChannel.read(contentBuffer);
            contentBuffer.flip();
            return contentBuffer;
        } catch (IOException e) {
            throw new StorageException(e);
        }

    }

    @Override
    public synchronized LogEntryInfo getLast() {
        return new LogEntryInfo().index(lastIndex.get()).term(lastTerm.get());
    }

    @Override
    public synchronized void truncateFromIndex(long index) {

        if (index > 0) {
            try {
                // TODO truncate from contentFileChannel
                metadataFileAppendLogChannel.truncate((index - 1) * INDEX_TERM_FILE_ENTRY_SIZE);
                lastIndex.set(index - 1);
                lastTerm.set(getTermByIndex(index - 1));
            } catch (IOException | BufferUnderflowException e) {
                throw new StorageException(e);
            }
        } else {
            lastIndex.set(0);
            lastTerm.set(0);
        }
    }

    @Override
    public void close() {
        try {
            nodeDataFile.close();
            metadataFileChannel.close();
            metadataFileAppendLogChannel.close();
            metadataFile.close();
            contentFileChannel.close();
            contentFileAppendLogChannel.close();
            contentFile.close();
            contentFileAppendLog.close();
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    private String filePath(String filePattern, int nodeId) {
        return Paths.get(this.directory.toString(), String.format(filePattern, nodeId)).toString();
    }
}
