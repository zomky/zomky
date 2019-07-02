package io.github.pmackowski.rsocket.raft;

import com.google.protobuf.ByteString;
import io.github.pmackowski.rsocket.raft.rpc.AppendEntriesRequest;
import io.github.pmackowski.rsocket.raft.rpc.AppendEntriesResponse;
import io.github.pmackowski.rsocket.raft.storage.RaftStorage;
import io.github.pmackowski.rsocket.raft.storage.StorageException;
import io.github.pmackowski.rsocket.raft.storage.log.reader.BoundedLogStorageReader;
import io.rsocket.Payload;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.retry.Repeat;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class LeaderRole implements RaftServerRole {

    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderRole.class);

    private static final Duration HEARTBEAT_TIMEOUT = Duration.ofMillis(100);

    private Function<Flux<Long>, ? extends Publisher<?>> repeatFactory;

    /**
     * Reinitialized after election,
     * index of the next log entry
     * to send to that server (initialized to leader
     * last log index + 1)
     */
    private Map<Integer, Long> nextIndex = new ConcurrentHashMap<>();

    /**
     * Reinitialized after election,
     * index of highest log entry
     * known to be replicated on server
     * (initialized to 0, increases monotonically)
     */
    private Map<Integer, Long> matchIndex = new ConcurrentHashMap<>();

    private Map<Integer, Disposable> heartbeats = new ConcurrentHashMap<>();

    private Map<Integer, BoundedLogStorageReader> logStorageReaders = new ConcurrentHashMap<>();

    private CommitIndexCalculator commitIndexCalculator;

    public LeaderRole() {
        this(HEARTBEAT_TIMEOUT);
    }

    public LeaderRole(Duration heartbeatTimeout) {
        this(Repeat.onlyIf(objectRepeatContext -> true).fixedBackoff(heartbeatTimeout));
    }

    LeaderRole(Function<Flux<Long>, ? extends Publisher<?>> repeatFactory) {
        this(repeatFactory, new CommitIndexCalculator());
    }

    LeaderRole(Function<Flux<Long>, ? extends Publisher<?>> repeatFactory, CommitIndexCalculator commitIndexCalculator) {
        this.repeatFactory = repeatFactory;
        this.commitIndexCalculator = commitIndexCalculator;
    }

    @Override
    public NodeState nodeState() {
        return NodeState.LEADER;
    }

    @Override
    public void onInit(DefaultRaftServer node, RaftStorage raftStorage) {
        node.availableSenders().subscribe(sender -> initHeartbeats(node, raftStorage, sender));
        node.onSenderAvailable(sender -> initHeartbeats(node, raftStorage, sender));

        node.onSenderUnavailable(sender -> {
            LOGGER.info("[RaftServer {}] Sender unavailable {}", node.nodeId, sender.getNodeId());
            Disposable disposable = heartbeats.remove(sender.getNodeId());
            if (disposable != null) {
                disposable.dispose();
            }
            BoundedLogStorageReader logStorageReader = logStorageReaders.remove(sender.getNodeId());
            if (logStorageReader != null) {
                logStorageReader.close();
            }
        });
    }

    @Override
    public void onExit(DefaultRaftServer node, RaftStorage raftStorage) {
        heartbeats.values().forEach(Disposable::dispose);
    }

    @Override
    public Mono<Payload> onClientRequest(DefaultRaftServer raftServer, RaftStorage raftStorage, Payload payload) {
        return onClientRequests(raftServer, raftStorage, Mono.just(payload)).next();
    }

    @Override
    public Flux<Payload> onClientRequests(DefaultRaftServer raftServer, RaftStorage raftStorage, Publisher<Payload> payloads) {
        // TODO it has nothing to do with state machine, should depend on the client
        return (raftServer.stateMachine != null) ?
            new SenderLastAppliedOperator(payloads, raftServer, raftStorage) :
            new SenderConfirmOperator(payloads, raftServer, raftStorage);
    }

    @Override
    public Mono<Payload> onAddServer(DefaultRaftServer raftServer, RaftStorage raftStorage, Payload payload) {
        return null;
    }

    private void initHeartbeats(DefaultRaftServer node, RaftStorage raftStorage, Sender sender) {
        LOGGER.info("[RaftServer {}] Sender available {}", node.nodeId, sender.getNodeId());
        try {
            long lastLogIndex = raftStorage.getLastIndexedTerm().getIndex();
            long nextIdx = lastLogIndex + 1;
            nextIndex.put(sender.getNodeId(), nextIdx);
            matchIndex.put(sender.getNodeId(), 0L);
            heartbeats.put(sender.getNodeId(), heartbeats(sender, raftStorage, node).subscribe());
            logStorageReaders.put(sender.getNodeId(), new BoundedLogStorageReader(raftStorage.openReader(nextIdx)));
        } catch (Exception e) {
            LOGGER.error("initHeartbeats", e);
            throw new StorageException(e);
        }
    }

    private Flux<AppendEntriesResponse> heartbeats(Sender sender, RaftStorage raftStorage, DefaultRaftServer node) {
        return Mono.defer(() -> Mono.just(heartbeatRequest(sender, node, raftStorage)))
                   .flatMap(appendEntriesRequest -> sender.appendEntries(appendEntriesRequest)
                             .doOnNext(appendEntriesResponse -> {
                                 if (appendEntriesResponse.getTerm() > raftStorage.getTerm()) {
                                     node.convertToFollower(appendEntriesResponse.getTerm());
                                 }
                                 if (appendEntriesResponse.getSuccess()) {
                                     // If successful: update nextIndex and matchIndex for follower
                                     long lastLogIndex = appendEntriesRequest.getPrevLogIndex() + appendEntriesRequest.getEntriesCount();
                                     long nextIdx = lastLogIndex + 1;
                                     nextIndex.put(sender.getNodeId(), nextIdx);
                                     matchIndex.put(sender.getNodeId(), lastLogIndex);

                                     long candidateCommitIndex = commitIndexCalculator.calculate(raftStorage, node, matchIndex, lastLogIndex);
                                     if (candidateCommitIndex > node.getCommitIndex()) {
                                         node.setCommitIndex(candidateCommitIndex);
                                     }
                                 } else {
                                     // If AppendEntries fails because of log inconsistency decrement nextIndex and retry
                                     // TODO now retry is done in next heartbeat
                                     LOGGER.info("[RaftServer {}] Decrease nextIndex for sender {}", node.nodeId, sender.getNodeId());
                                     long next = nextIndex.get(sender.getNodeId()) - 1;
                                     nextIndex.put(sender.getNodeId(), Math.min(next, 1));
                                 }
                             })
                            .doOnError(throwable -> LOGGER.error("Error while heartbeat!!", throwable))
                   )
                   .repeatWhen(repeatFactory);
    }

    private AppendEntriesRequest heartbeatRequest(Sender sender, DefaultRaftServer node, RaftStorage raftStorage) {
        BoundedLogStorageReader logStorageReader = logStorageReaders.get(sender.getNodeId());
        long senderIdxId = nextIndex.get(sender.getNodeId());
        long prevLogIndex = senderIdxId - 1;
        int prevLogTerm = raftStorage.getTermByIndex(prevLogIndex);

        // If last log index ≥ nextIndex for a follower: send
        // AppendEntries RPC with log entries starting at nextIndex
        AppendEntriesRequest.Builder builder = AppendEntriesRequest.newBuilder();
        long lastIndex = raftStorage.getLastIndexedTerm().getIndex();
        if (lastIndex >= senderIdxId) {
            Iterable<ByteBuffer> entries = logStorageReader.getRawEntriesByIndex(senderIdxId, lastIndex);
            entries.forEach(rawLogEntry -> builder.addEntries(ByteString.copyFrom(rawLogEntry))); // always copy as ByteString is immutable
        }

        return builder
                .setTerm(raftStorage.getTerm())
                .setLeaderId(node.nodeId)
                .setPrevLogIndex(prevLogIndex)
                .setPrevLogTerm(prevLogTerm)
                .setLeaderCommit(node.getCommitIndex())
                .build();
    }

}
