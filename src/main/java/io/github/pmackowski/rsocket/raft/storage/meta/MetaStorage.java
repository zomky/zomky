package io.github.pmackowski.rsocket.raft.storage.meta;

import io.github.pmackowski.rsocket.raft.storage.RaftStorageConfiguration;
import io.github.pmackowski.rsocket.raft.storage.StorageException;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

public class MetaStorage implements AutoCloseable{

    private static final String ZOMKY_NODE_DATA = "meta.data";

    private RaftStorageConfiguration configuration;

    private RandomAccessFile nodeDataFile;
    private FileChannel nodeDataFileChannel;

    public MetaStorage(RaftStorageConfiguration configuration) {
        this.configuration = configuration;
        initialize();
    }

    private void initialize() {
        try {
            nodeDataFile = new RandomAccessFile(filePath(ZOMKY_NODE_DATA), "rw");
            nodeDataFileChannel = nodeDataFile.getChannel();
            if (nodeDataFileChannel.size() == 0) {
                update(0, 0);
            }
        } catch (Exception e) {
            throw  new StorageException(e);
        }
    }

    public synchronized int getTerm() {
        try {
            ByteBuffer metadataBuffer = ByteBuffer.allocate(Integer.BYTES);
            nodeDataFileChannel.read(metadataBuffer, 0);
            metadataBuffer.flip();
            return metadataBuffer.getInt();
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    public synchronized int getVotedFor() {
        try {
            ByteBuffer metadataBuffer = ByteBuffer.allocate(Integer.BYTES);
            nodeDataFileChannel.read(metadataBuffer, Integer.BYTES);
            metadataBuffer.flip();
            return metadataBuffer.getInt();
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    public synchronized Configuration getConfiguration() {
        try {
            int members = (int) (nodeDataFileChannel.size() / Integer.BYTES - 2);
            ByteBuffer metadataBuffer = ByteBuffer.allocate(Integer.BYTES * members);
            nodeDataFileChannel.read(metadataBuffer, Integer.BYTES * 2);
            metadataBuffer.flip();
            Configuration configuration = new Configuration();
            while (metadataBuffer.hasRemaining()) {
                configuration = configuration.addMember(metadataBuffer.getInt());
            }
            return configuration.getMembers().isEmpty() ? null : configuration; // TODO
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

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

    public synchronized void updateConfiguration(Configuration configuration) {
        try {
            nodeDataFileChannel.position(2 * Integer.BYTES);
            ByteBuffer byteBuffer = ByteBuffer.allocate(configuration.membersCount() * Integer.BYTES);
            configuration.getMembers().forEach(member -> {
                byteBuffer.putInt(member);
            });
            byteBuffer.flip();
            nodeDataFileChannel.write(byteBuffer);
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void close() {
        try {
            nodeDataFile.close();
            nodeDataFileChannel.close();
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    private String filePath(String filePattern) {
        return Paths.get(this.configuration.getDirectory().toString(), filePattern).toString();
    }
}
