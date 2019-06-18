package io.github.pmackowski.rsocket.raft.storage;

import io.github.pmackowski.rsocket.raft.storage.log.Segment;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class RaftStorageUtils {

    public static FileChannel openChannel(Path directory, String filename) {
        try {
            Path path = Paths.get(directory.toAbsolutePath().toString(), filename);
            return FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    public static FileChannel openChannel(Path path) {
        try {
            return FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    public static MappedByteBuffer mapReadWrite(FileChannel fileChannel, Segment segment) {
        return map(fileChannel, segment, FileChannel.MapMode.READ_WRITE);
    }

    public static MappedByteBuffer mapReadOnly(FileChannel fileChannel, Segment segment) {
        return map(fileChannel, segment, FileChannel.MapMode.READ_ONLY);
    }

    private static MappedByteBuffer map(FileChannel fileChannel, Segment segment, FileChannel.MapMode mapMode) {
        try {
            return fileChannel.map(mapMode, 0, segment.getSegmentHeader().getSegmentSize());
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    public static int getInt(FileChannel channel, long position) {
        try {
            ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);
            channel.read(byteBuffer, position);
            byteBuffer.flip();
            return byteBuffer.getInt();
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

}
