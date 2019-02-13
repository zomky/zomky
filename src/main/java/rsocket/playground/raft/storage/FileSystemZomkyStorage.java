package rsocket.playground.raft.storage;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Default implementation
 * TODO remove synchronized
 */
public class FileSystemZomkyStorage implements ZomkyStorage {

    private static final int INDEX_TERM_FILE_ENTRY_SIZE = Long.BYTES + 2 * Integer.BYTES;

    private static final String ZOMKY_DIRECTORY = ".zomky";
    private static final String ZOMKY_NODE_DATA = "node.%s.zomky";
    private static final String ZOMKY_LOG_ENTRIES_METADATA = "node.%s.entries.metadata.zomky";
    private static final String ZOMKY_LOG_ENTRIES_CONTENT = "node.%s.entries.content.zomky";

    private Path directory;

    private RandomAccessFile nodeDataFile;
    private FileChannel nodeDataFileChannel;
    private RandomAccessFile metadataFile;
    private FileChannel metadataFileChannel;
    private RandomAccessFile contentFile;
    private FileChannel contentFileChannel;
    private ByteBuffer byteBuffer = ByteBuffer.allocate(INDEX_TERM_FILE_ENTRY_SIZE);

    private AtomicLong lastIndex;
    private AtomicInteger lastTerm;

    public FileSystemZomkyStorage(int nodeId) {
        this(nodeId, System.getProperty("user.home"));
    }

    public FileSystemZomkyStorage(int nodeId, String directory) {
        this.directory = Paths.get(directory, ZOMKY_DIRECTORY);
        try {
            if (Files.notExists(this.directory)) {
                Files.createDirectory(this.directory);
            }
        } catch (IOException e) {
            throw new ZomkyStorageException(e);
        }

        try {
            nodeDataFile = new RandomAccessFile(filePath(ZOMKY_NODE_DATA, nodeId), "rw");
            nodeDataFileChannel = nodeDataFile.getChannel();
            metadataFile = new RandomAccessFile(filePath(ZOMKY_LOG_ENTRIES_METADATA, nodeId), "rw");
            metadataFileChannel = metadataFile.getChannel();
            contentFile = new RandomAccessFile(filePath(ZOMKY_LOG_ENTRIES_CONTENT, nodeId), "rw");
            contentFileChannel = contentFile.getChannel();

            if (nodeDataFileChannel.size() == 0) {
                update(0, 0);
            }

            lastIndex = new AtomicLong(metadataFileChannel.size() / INDEX_TERM_FILE_ENTRY_SIZE);
            if (lastIndex.get() > 0) {
                lastTerm = new AtomicInteger(getTermByIndex(lastIndex.get()));
            } else {
                lastTerm = new AtomicInteger(0);
            }

            contentFileChannel.position(lastIndex.get() * INDEX_TERM_FILE_ENTRY_SIZE);
        } catch (Exception e) {
            close();
            throw new ZomkyStorageException(e);
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
            throw new ZomkyStorageException(e);
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
            throw new ZomkyStorageException(e);
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
            throw new ZomkyStorageException(e);
        }
    }

    @Override
    public synchronized LogEntryInfo appendLog(int term, ByteBuffer buffer) {
        try {
            long position = contentFileChannel.size();
            int messageSize = buffer.remaining();
            contentFileChannel.write(buffer);

            byteBuffer.position(0);
            byteBuffer.putInt(term);
            byteBuffer.putLong(position);
            byteBuffer.putInt(messageSize);
            byteBuffer.flip();
            metadataFileChannel.write(byteBuffer);
//            metadataFileChannel.force(true);
            lastTerm.set(term);
            return new LogEntryInfo()
                    .index(lastIndex.incrementAndGet())
                    .term(term);
        } catch (IOException e) {
            throw new ZomkyStorageException(e);
        }
    }

    @Override
    public synchronized int getTermByIndex(long index) {
        try {
            metadataFileChannel.position((index-1) * INDEX_TERM_FILE_ENTRY_SIZE);
            ByteBuffer metadataBuffer = ByteBuffer.allocate(Integer.BYTES);
            metadataFileChannel.read(metadataBuffer);
            metadataBuffer.flip();
            return metadataBuffer.getInt();
        } catch (IOException | BufferUnderflowException e) {
            throw new ZomkyStorageException(e);
        }
    }

    @Override
    public synchronized ByteBuffer getEntryByIndex(long index) {
        try {
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
            throw new ZomkyStorageException(e);
        }
    }

    @Override
    public synchronized LogEntryInfo getLast() {
        return new LogEntryInfo().index(lastIndex.get()).term(lastTerm.get());
    }

    @Override
    public void close() {
        try {
            metadataFileChannel.close();
            metadataFile.close();
            contentFileChannel.close();
            contentFile.close();
        } catch (Exception e) {
            throw new ZomkyStorageException(e);
        }
    }

    private String filePath(String filePattern, int nodeId) {
        return Paths.get(this.directory.toString(), String.format(filePattern, nodeId)).toString();
    }
}
