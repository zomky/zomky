package io.github.pmackowski.rsocket.raft.raft;

import com.google.protobuf.ByteString;
import io.github.pmackowski.rsocket.raft.InnerNode;
import io.github.pmackowski.rsocket.raft.storage.RaftStorage;
import io.github.pmackowski.rsocket.raft.storage.StorageException;
import io.github.pmackowski.rsocket.raft.storage.log.reader.BoundedLogStorageReader;
import io.github.pmackowski.rsocket.raft.transport.Sender;
import io.github.pmackowski.rsocket.raft.transport.protobuf.*;
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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

// TODO heartbeats groups coalescing, current solution is inefficient
public class LeaderRole implements RaftRole {

    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderRole.class);

    private static final Duration HEARTBEAT_TIMEOUT = Duration.ofMillis(100);
    private static final int CATCH_UP_MAX_ROUNDS = 3;

    private Function<Flux<Long>, ? extends Publisher<?>> repeatFactory;
    private int catchUpMaxRounds;
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
        this(HEARTBEAT_TIMEOUT, CATCH_UP_MAX_ROUNDS);
    }

    public LeaderRole(int catchUpMaxRounds) {
        this(HEARTBEAT_TIMEOUT, catchUpMaxRounds);
    }

    public LeaderRole(Duration heartbeatTimeout) {
        this(Repeat.onlyIf(objectRepeatContext -> true).fixedBackoff(heartbeatTimeout), CATCH_UP_MAX_ROUNDS);
    }

    public LeaderRole(Duration heartbeatTimeout, int catchUpMaxRounds) {
        this(Repeat.onlyIf(objectRepeatContext -> true).fixedBackoff(heartbeatTimeout), catchUpMaxRounds);
    }

    LeaderRole(Function<Flux<Long>, ? extends Publisher<?>> repeatFactory) {
        this(repeatFactory, CATCH_UP_MAX_ROUNDS);
    }

    LeaderRole(Function<Flux<Long>, ? extends Publisher<?>> repeatFactory, int catchUpMaxRounds) {
        this(repeatFactory, new CommitIndexCalculator(), catchUpMaxRounds);
    }

    LeaderRole(Function<Flux<Long>, ? extends Publisher<?>> repeatFactory, CommitIndexCalculator commitIndexCalculator, int catchUpMaxRounds) {
        this.repeatFactory = repeatFactory;
        this.commitIndexCalculator = commitIndexCalculator;
        this.catchUpMaxRounds = catchUpMaxRounds;
    }

    @Override
    public NodeState nodeState() {
        return NodeState.LEADER;
    }

    @Override
    public void onInit(InnerNode node, RaftGroup raftGroup, RaftStorage raftStorage) {
        raftGroup.availableSenders().subscribe(sender -> initHeartbeats(node, raftGroup, raftStorage, sender));
        node.onSenderAvailable(sender -> {
            if (sender.getNodeId() == node.getNodeId()) {
                return;
            }
            if (raftGroup.getCurrentConfiguration().getMembers().contains(sender.getNodeId())) {
                initHeartbeats(node, raftGroup, raftStorage, sender);
            }
        });

        node.onSenderUnavailable(sender -> {
            if (sender.getNodeId() == node.getNodeId()) {
                return;
            }
            if (raftGroup.getCurrentConfiguration().getMembers().contains(sender.getNodeId())) {
                LOGGER.info("[Node {}] Sender unavailable {}", node.getNodeId(), sender.getNodeId());
                Disposable disposable = heartbeats.remove(sender.getNodeId());
                if (disposable != null) {
                    disposable.dispose();
                }
                BoundedLogStorageReader logStorageReader = logStorageReaders.remove(sender.getNodeId());
                if (logStorageReader != null) {
                    logStorageReader.close();
                }
            }
        });
    }

    @Override
    public void onExit(InnerNode node, RaftGroup raftGroup, RaftStorage raftStorage) {
        heartbeats.values().forEach(Disposable::dispose);
    }

    @Override
    public Mono<Payload> onClientRequest(InnerNode node, RaftGroup raftGroup, RaftStorage raftStorage, Payload payload) {
        return onClientRequests(node, raftGroup, raftStorage, Mono.just(payload)).next();
    }

    @Override
    public Flux<Payload> onClientRequests(InnerNode node, RaftGroup raftGroup, RaftStorage raftStorage, Publisher<Payload> payloads) {
        // TODO it has nothing to do with state machine, should depend on the client
        return (raftGroup.getRaftConfiguration().getStateMachine() != null) ?
            new SenderLastAppliedOperator(payloads, raftGroup, raftStorage) :
            new SenderConfirmOperator(payloads, raftGroup, raftStorage);
    }

    @Override
    public Mono<RemoveServerResponse> onRemoveServer(InnerNode node, RaftGroup raftGroup, RaftStorage raftStorage, RemoveServerRequest removeServerRequest) {
        return Mono.just(removeServerRequest)
                .doOnNext(i -> raftGroup.removeServer(removeServerRequest))
                .doOnError(throwable -> LOGGER.error("Remove server has failed!", throwable))
                .thenReturn(RemoveServerResponse.newBuilder().setLeaderHint(node.getNodeId()).setStatus(true).build())
                .onErrorReturn(RemoveServerResponse.newBuilder().setLeaderHint(node.getNodeId()).setStatus(false).build());
    }

    @Override
    public Mono<AddServerResponse> onAddServer(InnerNode node, RaftGroup raftGroup, RaftStorage raftStorage, AddServerRequest addServerRequest) {
        // Catch up new server for fixed number of rounds. Reply TIMEOUT
        // if new server does not make progress for an election timeout or
        // if the last round takes longer than election timeout

        final BoundedLogStorageReader logStorageReader = new BoundedLogStorageReader(raftStorage.openReader());
        CatchUpContext catchUpContext = new CatchUpContext(senderNextIndex(raftStorage), catchUpMaxRounds);
        return raftGroup.getSenderById(addServerRequest.getNewServer()).flatMap(sender -> {
                final AppendEntriesRequest appendEntriesRequest = appendEntriesRequest(catchUpContext.getSenderNextIndex(), node, raftGroup, raftStorage, logStorageReader);
                return sender.appendEntries(raftGroup, appendEntriesRequest)
                        .doOnNext(appendEntriesResponse -> {
                            // first response almost always will be false
                            boolean success = appendEntriesResponse.getSuccess();
                            long lastLogIndex = success ?
                                    appendEntriesRequest.getPrevLogIndex() + appendEntriesRequest.getEntriesCount() :
                                    appendEntriesResponse.getLastLogIndex();
                            long senderNextIndex = lastLogIndex + 1;
                            catchUpContext.setSenderNextIndex(senderNextIndex);
                            if (success) {
                                catchUpContext.incrementRound();
                            } else {
                                catchUpContext.logFailure();
                            }
                        })
                        .doOnError(throwable -> LOGGER.error("Error while catching up!!", throwable));
            })
            .elapsed()
            .repeatWhen(lessThanMaxRounds(catchUpContext))
            .last()
            .doOnNext(tuple -> {
                long elapsedTime = tuple.getT1();
                if (elapsedTime > ElectionTimeout.ELECTION_TIMEOUT_MIN_IN_MILLIS) {
                    throw new RaftException(new TimeoutException());
                }
            })
            .doOnNext(i -> raftGroup.addServer(addServerRequest))
            .doOnError(throwable -> LOGGER.error("Add server has failed!", throwable))
            .thenReturn(AddServerResponse.newBuilder().setLeaderHint(node.getNodeId()).setStatus(true).build())
            .onErrorReturn(AddServerResponse.newBuilder().setLeaderHint(node.getNodeId()).setStatus(false).build());
    }

    private Repeat<CatchUpContext> lessThanMaxRounds(CatchUpContext catchUpContext) {
        return Repeat.<CatchUpContext>onlyIf(repeatContext -> repeatContext.applicationContext().repeat())
                .withApplicationContext(catchUpContext);
    }

    private static class CatchUpContext {

        private int maxRounds;
        private volatile long senderNextIndex;
        private volatile long rounds;
        private AtomicBoolean logFailure = new AtomicBoolean(false);

        CatchUpContext(long senderNextIndex, int maxRounds) {
            LOGGER.info("senderNextIndex {}", senderNextIndex);
            this.senderNextIndex = senderNextIndex;
            this.maxRounds = maxRounds;
        }

        long getSenderNextIndex() {
            return senderNextIndex;
        }

        boolean repeat() {
            return rounds < maxRounds;
        }

        void setSenderNextIndex(long senderNextIndex) {
            LOGGER.info("setSenderNextIndex {}", senderNextIndex);
            this.senderNextIndex = senderNextIndex;
        }

        void incrementRound() {
            LOGGER.info("incrementRound");
            this.rounds = rounds + 1;
        }

        void logFailure() {
            LOGGER.info("logFailure");
            if (!logFailure.compareAndSet(false, true)) {
                throw new RaftException("Server cannot be added");
            }
        }
    }

    private void initHeartbeats(InnerNode node, RaftGroup raftGroup, RaftStorage raftStorage, Sender sender) {
        LOGGER.info("[Node {}] Sender available {}", node.getNodeId(), sender.getNodeId());
        try {
            long senderNextIndex = senderNextIndex(raftStorage);
            nextIndex.put(sender.getNodeId(), senderNextIndex);
            matchIndex.put(sender.getNodeId(), 0L);
            heartbeats.put(sender.getNodeId(), heartbeats(sender, raftStorage, node, raftGroup).subscribe());
            logStorageReaders.put(sender.getNodeId(), new BoundedLogStorageReader(raftStorage.openReader(senderNextIndex)));
        } catch (Exception e) {
            LOGGER.error("initHeartbeats", e);
            throw new StorageException(e);
        }
    }

    private long senderNextIndex(RaftStorage raftStorage) {
        long lastLogIndex = raftStorage.getLastIndexedTerm().getIndex();
        return lastLogIndex + 1;
    }

    private Flux<AppendEntriesResponse> heartbeats(Sender sender, RaftStorage raftStorage, InnerNode node, RaftGroup raftGroup) {
        return Mono.defer(() -> Mono.just(heartbeatRequest(sender, node, raftGroup, raftStorage)))
                   .flatMap(appendEntriesRequest -> sender.appendEntries(raftGroup, appendEntriesRequest)
                             .doOnNext(appendEntriesResponse -> {
                                 if (appendEntriesResponse.getTerm() > raftStorage.getTerm()) {
                                     raftGroup.convertToFollower(appendEntriesResponse.getTerm());
                                 }
                                 if (appendEntriesResponse.getSuccess()) {
                                     // If successful: update nextIndex and matchIndex for follower
                                     long lastLogIndex = appendEntriesRequest.getPrevLogIndex() + appendEntriesRequest.getEntriesCount();
                                     long nextIdx = lastLogIndex + 1;
                                     nextIndex.put(sender.getNodeId(), nextIdx);
                                     matchIndex.put(sender.getNodeId(), lastLogIndex);

                                     long candidateCommitIndex = commitIndexCalculator.calculate(raftStorage, raftGroup, matchIndex, lastLogIndex);
                                     if (candidateCommitIndex > raftGroup.getCommitIndex()) {
                                         raftGroup.setCommitIndex(candidateCommitIndex);
                                     }
                                 } else {
                                     // If AppendEntries fails because of log inconsistency decrement nextIndex and retry
                                     // TODO now retry is done in next heartbeat
                                     LOGGER.info("[Node {}] Decrease nextIndex for sender {}", node.getNodeId(), sender.getNodeId());
                                     long next = nextIndex.get(sender.getNodeId()) - 1;
                                     nextIndex.put(sender.getNodeId(), Math.max(next, 1));
                                 }
                             })
                            .doOnError(throwable -> LOGGER.error("Error while heartbeat!!", throwable))
                   )
                   .repeatWhen(repeatFactory);
    }

    private AppendEntriesRequest heartbeatRequest(Sender sender, InnerNode node, RaftGroup raftGroup, RaftStorage raftStorage) {
        long senderNextIndex = nextIndex.get(sender.getNodeId());
        BoundedLogStorageReader logStorageReader = logStorageReaders.get(sender.getNodeId());
        return appendEntriesRequest(senderNextIndex, node, raftGroup, raftStorage, logStorageReader);
    }

    private AppendEntriesRequest appendEntriesRequest(long senderNextIndex, InnerNode node, RaftGroup raftGroup, RaftStorage raftStorage, BoundedLogStorageReader logStorageReader) {
        long prevLogIndex = senderNextIndex - 1;
        int prevLogTerm = raftStorage.getTermByIndex(prevLogIndex);

        // If last log index ≥ nextIndex for a follower: send
        // AppendEntries RPC with log entries starting at nextIndex
        AppendEntriesRequest.Builder builder = AppendEntriesRequest.newBuilder();
        long lastIndex = raftStorage.getLastIndexedTerm().getIndex();
        if (lastIndex >= senderNextIndex) {
            Iterable<ByteBuffer> entries = logStorageReader.getRawEntriesByIndex(senderNextIndex, lastIndex);
            entries.forEach(rawLogEntry -> builder.addEntries(ByteString.copyFrom(rawLogEntry))); // always copy as ByteString is immutable
        }

        return builder
                .setTerm(raftStorage.getTerm())
                .setLeaderId(node.getNodeId())
                .setPrevLogIndex(prevLogIndex)
                .setPrevLogTerm(prevLogTerm)
                .setLeaderCommit(raftGroup.getCommitIndex())
                .build();
    }

}
