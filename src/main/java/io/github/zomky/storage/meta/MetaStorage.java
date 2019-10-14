package io.github.zomky.storage.meta;

import io.github.zomky.storage.RaftStorageConfiguration;
import io.github.zomky.storage.StorageException;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MetaStorage implements AutoCloseable {

    private static final String ZOMKY_NODE_DATA = "meta.data";

    private RaftStorageConfiguration configuration;

    private RandomAccessFile nodeDataFile;
    private FileChannel nodeDataFileChannel;
    private ReadWriteLock lock = new ReentrantReadWriteLock();

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
            throw new StorageException(e);
        }
    }

    public int getTerm() {
        lock.readLock().lock();
        try {
            ByteBuffer metadataBuffer = ByteBuffer.allocate(Integer.BYTES);
            nodeDataFileChannel.read(metadataBuffer, 0);
            metadataBuffer.flip();
            return metadataBuffer.getInt();
        } catch (IOException e) {
            throw new StorageException(e);
        } finally {
            lock.readLock().unlock();
        }
    }

    public int getVotedFor() {
        lock.readLock().lock();
        try {
            ByteBuffer metadataBuffer = ByteBuffer.allocate(Integer.BYTES);
            nodeDataFileChannel.read(metadataBuffer, Integer.BYTES);
            metadataBuffer.flip();
            return metadataBuffer.getInt();
        } catch (IOException e) {
            throw new StorageException(e);
        } finally {
            lock.readLock().unlock();
        }
    }

    public Configuration getConfiguration() {
        lock.readLock().lock();
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
        } finally {
            lock.readLock().unlock();
        }
    }

    public void update(int term, int votedFor) {
        lock.writeLock().lock();
        try {
            nodeDataFileChannel.position(0);
            ByteBuffer byteBuffer = ByteBuffer.allocate(2 * Integer.BYTES);
            byteBuffer.putInt(term);
            byteBuffer.putInt(votedFor);
            byteBuffer.flip();
            nodeDataFileChannel.write(byteBuffer);
        } catch (IOException e) {
            throw new StorageException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void updateConfiguration(Configuration configuration) {
        lock.writeLock().lock();
        try {
            nodeDataFileChannel.position(2 * Integer.BYTES);
            ByteBuffer byteBuffer = ByteBuffer.allocate(configuration.membersCount() * Integer.BYTES);
            configuration.getMembers().forEach(byteBuffer::putInt);
            byteBuffer.flip();
            nodeDataFileChannel.write(byteBuffer);
        } catch (IOException e) {
            throw new StorageException(e);
        } finally {
            lock.writeLock().unlock();
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
