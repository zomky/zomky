package io.github.pmackowski.rsocket.raft.integration;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.github.pmackowski.rsocket.raft.Node;
import io.github.pmackowski.rsocket.raft.NodeFactory;
import io.github.pmackowski.rsocket.raft.external.protobuf.CommandRequest;
import io.github.pmackowski.rsocket.raft.gossip.Cluster;
import io.github.pmackowski.rsocket.raft.storage.FileSystemRaftStorage;
import io.github.pmackowski.rsocket.raft.storage.RaftStorage;
import io.github.pmackowski.rsocket.raft.storage.RaftStorageConfiguration;
import io.github.pmackowski.rsocket.raft.storage.log.SizeUnit;
import io.github.pmackowski.rsocket.raft.storage.log.entry.CommandEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.LogEntry;
import reactor.blockhound.BlockHound;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.github.pmackowski.rsocket.raft.storage.log.serializer.LogEntrySerializer.serialize;

public class IntegrationTestsUtils {

    public static void checkBlockingCalls() {
        BlockHound.builder()
                .allowBlockingCallsInside("java.io.FileInputStream", "readBytes")
                .install();
    }

    public static RaftStorage raftStorage(Path directory) {
        return  raftStorage(directory, "1");
    }

    public static RaftStorage raftStorage(Path directory, String node) {
        return new FileSystemRaftStorage(RaftStorageConfiguration.builder()
                .segmentSize(SizeUnit.megabytes, 1)
                .directory(Paths.get(directory.toAbsolutePath().toString(), "node" + node))
                .build()
        );
    }

    public static ByteString entry(int term, String key, String value) {
        final CommandRequest commandRequest = CommandRequest.newBuilder().setKey(key).setValue(value).setSetOperation(true).buildPartial();
        CommandEntry commandEntry = new CommandEntry(term, System.currentTimeMillis(), commandRequest.toByteArray());
        ByteBuffer byteBuffer = ByteBuffer.allocate(LogEntry.SIZE + commandRequest.toByteArray().length + 1);
        serialize(commandEntry, byteBuffer);
        byteBuffer.flip();

        return ByteString.copyFrom(byteBuffer);
    }

    public static String logEntry(RaftStorage raftStorage, long index) {
        return raftStorage.getEntryByIndex(index)
                .map(indexedLogEntry -> {
                    CommandEntry commandEntry = (CommandEntry) indexedLogEntry.getLogEntry();
                    try {
                        CommandRequest commandRequest = CommandRequest.parseFrom(commandEntry.getValue());
                        String keyValue = String.format("%s=%s", commandRequest.getKey(), commandRequest.getValue());
//                        CommandEntry newCommandEntry = new CommandEntry(commandEntry.getTerm(), commandEntry.getTimestamp(), keyValue.getBytes());
//                        return new IndexedLogEntry(newCommandEntry, indexedLogEntry.getIndex(), indexedLogEntry.getSize());
                        return keyValue;
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException(e);
                    }
                })
                .orElse(null);
    }

    public static Map<Integer, Node> startNodes(int numberOfNodes, int firstPort) {
        Cluster cluster = new Cluster(IntStream.range(0, numberOfNodes).map(i -> firstPort +i).toArray());
        return IntStream.range(0, numberOfNodes)
                .mapToObj(i -> NodeFactory.receive()
                        .port(firstPort + i)
                        .cluster(cluster)
                        .start()
                        .block())
                .collect(Collectors.toMap(Node::getNodeId, n -> n));
    }

}
