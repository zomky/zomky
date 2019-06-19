package io.github.pmackowski.rsocket.raft.storage;

import io.github.pmackowski.rsocket.raft.storage.log.entry.CommandEntry;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

public class RaftStorageBenchmarkUtils {

    public static Path createTempDirectory() throws IOException {
        return Files.createTempDirectory("jmh");
    }

    public static void delete(Path directory) throws IOException {
        Files.walk(directory)
                .map(Path::toFile)
                .sorted((o1, o2) -> -o1.compareTo(o2))
                .forEach(File::delete);
    }

    public static CommandEntry commandEntry(int term, long timestamp, int entrySize) {
        int termAndTimestampSize = Integer.BYTES + Long.BYTES;
        final String value = String.join("", Collections.nCopies(entrySize - termAndTimestampSize, "a"));
        return new CommandEntry(term, timestamp, value.getBytes());
    }

}
