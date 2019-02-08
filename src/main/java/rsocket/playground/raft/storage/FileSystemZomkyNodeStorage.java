package rsocket.playground.raft.storage;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rsocket.playground.raft.LogEntry;
import rsocket.playground.raft.NodeData;
import rsocket.playground.raft.RaftException;

import java.io.IOException;
import java.nio.file.*;
import java.util.stream.BaseStream;

public class FileSystemZomkyNodeStorage implements ZomkyNodeStorage {

    private static final String ZOMKY_DIRECTORY = ".zomky";
    private static final String ZOMKY_NODE_DATA = "node.%s";
    private static final String ZOMKY_LOG_ENTRIES = "node.log.entries.%s";

    private Path directory;

    public FileSystemZomkyNodeStorage() {
        this(System.getProperty("user.home"));
    }

    public FileSystemZomkyNodeStorage(String directory) {
        this.directory = Paths.get(directory, ZOMKY_DIRECTORY);
        try {
            if (Files.notExists(this.directory)) {
                Files.createDirectory(this.directory);
            }
        } catch (Exception e) {

        }
    }

    @Override
    public void voteForMyself(int nodeId) {
        NodeData nodeData = readNodeData(nodeId);
        nodeData.increaseCurrentTerm();
        nodeData.votedFor(nodeId);
        writeFile(nodeData);
    }

    @Override
    public void voteForCandidate(int nodeId, int candidateId, long term) {
        NodeData nodeData = readNodeData(nodeId);
        nodeData.currentTerm(term);
        nodeData.votedFor(candidateId);
        writeFile(nodeData);
    }

    @Override
    public void updateTerm(int nodeId, long currentTerm) {
        NodeData nodeData = readNodeData(nodeId);
        nodeData.currentTerm(currentTerm);
        nodeData.votedFor(null);
        writeFile(nodeData);
    }

    @Override
    public NodeData readNodeData(int nodeId) {
        Path path = nodePath(nodeId);
        if (Files.notExists(path)) {
            try {
                Files.createFile(path);
            } catch (IOException e) {
                throw new RaftException("ddd", e);
            }
            NodeData nodeData = new NodeData()
                    .nodeId(nodeId)
                    .currentTerm(0);
            writeFile(nodeData);
            return nodeData;
        }
        try {
            byte[] data = Files.readAllBytes(path);
            String[] arr = new String(data).split(",");

            return new NodeData()
                    .nodeId(nodeId)
                    .currentTerm(Long.parseLong(arr[0]))
                    .votedFor("0".equals(arr[1]) ? null : Integer.parseInt(arr[1]));
        } catch (IOException e) {
            throw new RaftException("read failure", e);
        }
    }

    @Override
    public void appendLog(int nodeId, long index, long term, String content) {
        String contentToAppend = String.format("%s,%s,%s\n", index, term, content);
        try {
            Files.write(
                    logEntriesPath(nodeId),
                    contentToAppend.getBytes(),
                    StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new RaftException("appendLog failure", e);
        }
    }

    @Override // TODO just to play with algorithm, super inefficient
    public LogEntry getLast(int nodeId) {
        return getLogs(nodeId).last()
                .map(str -> {
                    String[] arr = str.split(",");
                    long index = Long.parseLong(arr[0]);
                    long term = Long.parseLong(arr[1]);
                    String content = arr[2];
                    return new LogEntry(index, term, content);
                })
                .switchIfEmpty(Mono.just(new LogEntry(0,0,null)))
                .block();
    }

    @Override // TODO just to play with algorithm, super inefficient
    public LogEntry getByIndex(int nodeId, long index) {
        return getLogs(nodeId)
                .map(str -> {
                    String[] arr = str.split(",");
                    long idx = Long.parseLong(arr[0]);
                    long term = Long.parseLong(arr[1]);
                    String content = arr[2];
                    return new LogEntry(idx, term, content);
                })
                .filter(logEntry -> logEntry.getIndex() == index)
                .switchIfEmpty(Mono.just(new LogEntry(0,0,null)))
                .blockFirst();
    }

    public Flux<String> getLogs(int nodeId) {
        return Flux.using(() -> Files.lines(logEntriesPath(nodeId)),
                Flux::fromStream,
                BaseStream::close
        );
    }

    private void writeFile(NodeData nodeData) {
        try {
            Path path = nodePath(nodeData.getNodeId());
            String content = String.format("%s,%s", nodeData.getCurrentTerm(), nodeData.getVotedFor() == null ? 0 : nodeData.getVotedFor());
            Files.write(path, content.getBytes(), StandardOpenOption.CREATE);
        } catch (IOException e) {
            throw new RaftException("write failure", e);
        }
    }

    private Path nodePath(int nodeId) {
        return Paths.get(directory.toString(), String.format(ZOMKY_NODE_DATA , nodeId));
    }

    private Path logEntriesPath(int nodeId) {
        return Paths.get(directory.toString(), String.format(ZOMKY_LOG_ENTRIES, nodeId));
    }

}
