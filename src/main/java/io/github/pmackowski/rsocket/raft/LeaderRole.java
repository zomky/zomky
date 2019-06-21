package io.github.pmackowski.rsocket.raft;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.github.pmackowski.rsocket.raft.rpc.AppendEntriesRequest;
import io.github.pmackowski.rsocket.raft.rpc.AppendEntriesResponse;
import io.github.pmackowski.rsocket.raft.storage.RaftStorage;
import io.github.pmackowski.rsocket.raft.storage.StorageException;
import io.github.pmackowski.rsocket.raft.storage.log.reader.BoundedLogStorageReader;
import io.github.pmackowski.rsocket.raft.utils.NettyUtils;
import io.rsocket.Payload;
import io.rsocket.util.ByteBufPayload;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.retry.Repeat;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class LeaderRole implements RaftServerRole {

    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderRole.class);

    private static final Duration HEARTBEAT_TIMEOUT = Duration.ofMillis(100);

    /**
     * Reinitialized after election,
     * index of the next log entry
     * to send to that server (initialized to leader
     * last log index + 1)
     */
    private ConcurrentMap<Integer, Long> nextIndex = new ConcurrentHashMap<>();

    /**
     * Reinitialized after election,
     * index of highest log entry
     * known to be replicated on server
     * (initialized to 0, increases monotonically)
     */
    private ConcurrentMap<Integer, Long> matchIndex = new ConcurrentHashMap<>();

    private ConcurrentMap<Integer, Disposable> heartbeats = new ConcurrentHashMap<>();

    private ConcurrentMap<Integer, BoundedLogStorageReader> logStorageReaders = new ConcurrentHashMap<>();

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
        if (raftServer.stateMachine != null) {
            return new SenderLastAppliedOperator(payloads, raftServer, raftStorage);
        } else {
            return new SenderConfirmOperator(payloads, raftServer, raftStorage);
        }
    }

    private void initHeartbeats(DefaultRaftServer node, RaftStorage raftStorage, Sender sender) {
        LOGGER.info("[RaftServer {}] Sender available {}", node.nodeId, sender.getNodeId());
        try {
            long lastLogIndex = raftStorage.getLast().getIndex();
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

    private Flux<Payload> heartbeats(Sender sender, RaftStorage raftStorage, DefaultRaftServer node) {
        return Mono.defer(() -> Mono.just(heartbeatRequest(sender, node, raftStorage)))
                   .flatMap(appendEntriesRequest -> sender.getAppendEntriesSocket()
                             .requestResponse(ByteBufPayload.create(appendEntriesRequest.toByteArray()))
                             .doOnNext(payload -> {
                                 AppendEntriesResponse appendEntriesResponse;
                                 try {
                                     appendEntriesResponse = AppendEntriesResponse.parseFrom(NettyUtils.toByteArray(payload.sliceData()));
                                 } catch (InvalidProtocolBufferException e) {
                                     throw new RaftException("Invalid append entries response!", e);
                                 }
                                 if (appendEntriesResponse.getTerm() > raftStorage.getTerm()) {
                                     node.convertToFollower(appendEntriesResponse.getTerm());
                                 }
                                 if (appendEntriesResponse.getSuccess()) {
                                     // If successful: update nextIndex and matchIndex for follower
                                     long lastLogIndex = appendEntriesRequest.getPrevLogIndex() + appendEntriesRequest.getEntriesCount();
                                     long nextIdx = lastLogIndex + 1;
                                     nextIndex.put(sender.getNodeId(), nextIdx);
                                     matchIndex.put(sender.getNodeId(), lastLogIndex);

                                     // If there exists an N such that N > commitIndex, a majority
                                     // of matchIndex[i] ≥ N, and log[N].term == currentTerm:
                                     // set commitIndex = N
                                     // TODO provide faster implementation
                                     long candidateCommitIndex = lastLogIndex;
                                     while (candidateCommitIndex > node.getCommitIndex()) {
                                         long committed = committed(candidateCommitIndex);
                                         // hardcoded, 1 means majority for 3 elements cluster
                                         int noCommittedRequired = 1;
                                         if (committed >= noCommittedRequired && raftStorage.getTermByIndex(candidateCommitIndex) == raftStorage.getTerm()) {
                                             break;
                                         } else {
                                             candidateCommitIndex--;
                                         }
                                     }
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
            ).repeatWhen(Repeat.onlyIf(objectRepeatContext -> true).fixedBackoff(HEARTBEAT_TIMEOUT));
    }

    private AppendEntriesRequest heartbeatRequest(Sender sender, DefaultRaftServer node, RaftStorage raftStorage) {
        BoundedLogStorageReader logStorageReader = logStorageReaders.get(sender.getNodeId());
        long senderIdxId = nextIndex.get(sender.getNodeId());
        long prevLogIndex = senderIdxId - 1;
        int prevLogTerm = raftStorage.getTermByIndex(prevLogIndex);

        // If last log index ≥ nextIndex for a follower: send
        // AppendEntries RPC with log entries starting at nextIndex
        AppendEntriesRequest.Builder builder = AppendEntriesRequest.newBuilder();
        long lastIndex = raftStorage.getLast().getIndex();
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

    private long committed(long idx) {
        return matchIndex.values().stream().filter(matchIndex1 -> matchIndex1 >= idx).count();
    }
}
