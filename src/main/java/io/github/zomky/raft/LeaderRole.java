package io.github.zomky.raft;

import com.google.protobuf.ByteString;
import io.github.zomky.Cluster;
import io.github.zomky.storage.RaftStorage;
import io.github.zomky.storage.StorageException;
import io.github.zomky.storage.log.reader.BoundedLogStorageReader;
import io.github.zomky.transport.Sender;
import io.github.zomky.transport.protobuf.*;
import io.rsocket.Payload;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.retry.Repeat;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

// TODO appendEntriesJob groups coalescing, current solution is inefficient
public class LeaderRole implements RaftRole {

    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderRole.class);

    private static final int MAX_BATCH_SIZE = Integer.MAX_VALUE;
    private static final Duration HEARTBEAT_TIMEOUT = Duration.ofMillis(200);
    private static final Duration DELAY_INTERVAL = Duration.ofMillis(20);
    private static final int CATCH_UP_MAX_ROUNDS = 3;

    private int catchUpMaxRounds;
    private Duration delayInterval;
    private Duration heartbeatTimeout;

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

    private Map<Integer, Disposable> appendEntriesJobs = new ConcurrentHashMap<>();

    private Map<Integer, BoundedLogStorageReader> logStorageReaders = new ConcurrentHashMap<>();

    private CommitIndexCalculator commitIndexCalculator;

    DirectProcessor<Long> newEntriesProcessor = DirectProcessor.create();
    FluxSink<Long> newEntriesSink = newEntriesProcessor.sink();
    Disposable heartbeatsDisposable;

    private volatile long lastNewEntry;

    public LeaderRole() {
        this(new CommitIndexCalculator(), DELAY_INTERVAL, HEARTBEAT_TIMEOUT, CATCH_UP_MAX_ROUNDS);
    }

    public LeaderRole(int catchUpMaxRounds) {
        this(new CommitIndexCalculator(), DELAY_INTERVAL, HEARTBEAT_TIMEOUT, catchUpMaxRounds);
    }

    public LeaderRole(Duration delayInterval, Duration heartbeatTimeout) {
        this(new CommitIndexCalculator(), delayInterval, heartbeatTimeout, CATCH_UP_MAX_ROUNDS);
    }

    public LeaderRole(CommitIndexCalculator commitIndexCalculator, Duration delayInterval, Duration heartbeatTimeout, int catchUpMaxRounds) {
        this.delayInterval = delayInterval;
        this.heartbeatTimeout = heartbeatTimeout;
        this.commitIndexCalculator = commitIndexCalculator;
        this.catchUpMaxRounds = catchUpMaxRounds;
    }

    @Override
    public NodeState nodeState() {
        return NodeState.LEADER;
    }

    @Override
    public void onInit(Cluster cluster, RaftGroup raftGroup, RaftStorage raftStorage) {
        raftGroup.availableSenders().subscribe(sender -> initAppendEntriesJob(cluster, raftGroup, raftStorage, sender));
        cluster.onSenderAvailable().subscribe(sender -> {
            if (raftGroup.getCurrentConfiguration().getMembers().contains(sender.getNodeId())) {
                initAppendEntriesJob(cluster, raftGroup, raftStorage, sender);
            }
        });

        cluster.onSenderUnavailable().subscribe(sender -> {
            if (raftGroup.getCurrentConfiguration().getMembers().contains(sender.getNodeId())) {
                removeAppendEntriesJob(cluster, raftGroup, sender);
            }
        });
        heartbeatsDisposable = heartbeatTimeout == Duration.ZERO ? Mono.empty().subscribe() :
                Flux.interval(Duration.ZERO, heartbeatTimeout)
                    .doOnNext(i -> {
                        if (lastNewEntry + heartbeatTimeout.toMillis() - delayInterval.toMillis() < System.currentTimeMillis()) {
                            newEntriesSink.next(i);
                        }
                    })
                    .subscribe();
    }

    @Override
    public void onExit(Cluster cluster, RaftGroup raftGroup, RaftStorage raftStorage) {
        newEntriesSink.complete();
        heartbeatsDisposable.dispose();
        appendEntriesJobs.values().forEach(Disposable::dispose);
    }

    @Override
    public Mono<Payload> onClientRequest(Cluster cluster, RaftGroup raftGroup, RaftStorage raftStorage, Payload payload) {
        return onClientRequests(cluster, raftGroup, raftStorage, Mono.just(payload)).next();
    }

    @Override
    public Flux<Payload> onClientRequests(Cluster cluster, RaftGroup raftGroup, RaftStorage raftStorage, Publisher<Payload> payloads) {
        // TODO it has nothing to do with state machine, should depend on the client
        return (raftGroup.getRaftConfiguration().getStateMachine() != null) ?
            new SenderLastAppliedOperator(payloads, raftGroup, raftStorage) :
            new SenderConfirmOperator(payloads, raftGroup, raftStorage);
    }

    @Override
    public void markNewEntry(long index, long timestamp) {
        newEntriesSink.next(index);
        lastNewEntry = timestamp;
    }

    @Override
    public Mono<RemoveServerResponse> onRemoveServer(Cluster cluster, RaftGroup raftGroup, RaftStorage raftStorage, RemoveServerRequest removeServerRequest) {
        return Mono.just(removeServerRequest) // TODO notify group follower
                .doOnNext(i -> raftGroup.removeServer(removeServerRequest))
                .doOnNext(i -> removeAppendEntriesJob(cluster, raftGroup, cluster.senderById(removeServerRequest.getOldServer())))
                .doOnError(throwable -> LOGGER.error("Remove server has failed!", throwable))
                .thenReturn(RemoveServerResponse.newBuilder().setLeaderHint(cluster.getLocalNodeId()).setStatus(true).build())
                .onErrorReturn(RemoveServerResponse.newBuilder().setLeaderHint(cluster.getLocalNodeId()).setStatus(false).build());
    }

    @Override
    public Mono<AddServerResponse> onAddServer(Cluster cluster, RaftGroup raftGroup, RaftStorage raftStorage, AddServerRequest addServerRequest) {
        // Catch up new server for fixed number of rounds. Reply TIMEOUT
        // if new server does not make progress for an election timeout or
        // if the last round takes longer than election timeout

        final BoundedLogStorageReader logStorageReader = new BoundedLogStorageReader(raftStorage.openReader());
        CatchUpContext catchUpContext = new CatchUpContext(senderNextIndex(raftStorage), catchUpMaxRounds);
        return raftGroup.getSenderById(addServerRequest.getNewServer())
                .flatMap(sender -> {
                    // TODO copy from raftGroup, now is hardcoded
                    // TODO a few AddGroupRequest attributes should be persisent (election timeout, persistent storage info, state machine name etc)
                    // now must be exactly the same as in RaftManagementClient except passive and passive attributes
                    AddGroupRequest addGroupRequest = AddGroupRequest.newBuilder()
                            .setElectionTimeoutMin((int) heartbeatTimeout.toMillis() * 3)
                            .setElectionTimeoutMax((int) heartbeatTimeout.toMillis() * 6)
                            .setPersistentStorage(false)
                            .setStateMachine("kv1")
                            .setPassive(true)
                            .build();
                    return sender.addGroup(raftGroup.getGroupName(), addGroupRequest).then(Mono.just(sender));
                })
                .flatMap(sender -> {
                    final AppendEntriesRequest appendEntriesRequest = appendEntriesRequest(catchUpContext.getSenderNextIndex(), cluster, raftGroup, raftStorage, logStorageReader);
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
            .log()
            .repeatWhen(lessThanMaxRounds(catchUpContext))
            .last()
            .doOnNext(tuple -> {
                long elapsedTime = tuple.getT1();
                Duration maxElectionTimeout = raftGroup.getCurrentElectionTimeout();
                if (elapsedTime > maxElectionTimeout.toMillis()) {
                    throw new RaftException(new TimeoutException());
                }
            })
            .doOnNext(i -> raftGroup.addServer(addServerRequest))
            // TODO refactoring
            .doOnNext(i -> initAppendEntriesJob(cluster, raftGroup, raftStorage, cluster.senderById(addServerRequest.getNewServer())))
            .doOnError(throwable -> LOGGER.error("Add server has failed!", throwable))
            .thenReturn(AddServerResponse.newBuilder().setLeaderHint(cluster.getLocalNodeId()).setStatus(true).build())
            .onErrorReturn(AddServerResponse.newBuilder().setLeaderHint(cluster.getLocalNodeId()).setStatus(false).build());
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

    private void initAppendEntriesJob(Cluster cluster, RaftGroup raftGroup, RaftStorage raftStorage, Sender sender) {
        LOGGER.info("[Node {}][group {}] Sender available {}", cluster.getLocalNodeId(), raftGroup.getGroupName(), sender.getNodeId());
        try {
            long senderNextIndex = senderNextIndex(raftStorage);
            nextIndex.put(sender.getNodeId(), senderNextIndex);
            matchIndex.put(sender.getNodeId(), 0L);
            appendEntriesJobs.put(sender.getNodeId(), appendEntriesJob(sender, raftStorage, cluster, raftGroup).subscribe());
            logStorageReaders.put(sender.getNodeId(), new BoundedLogStorageReader(raftStorage.openReader(senderNextIndex)));
        } catch (Exception e) {
            LOGGER.error("initAppendEntriesJob", e);
            throw new StorageException(e);
        }
    }

    private void removeAppendEntriesJob(Cluster cluster, RaftGroup raftGroup, Sender sender) {
        LOGGER.info("[Node {}][group {}] Sender unavailable {}", cluster.getLocalNodeId(), raftGroup.getGroupName(), sender.getNodeId());
        Disposable disposable = appendEntriesJobs.remove(sender.getNodeId());
        if (disposable != null) {
            disposable.dispose();
        }
        BoundedLogStorageReader logStorageReader = logStorageReaders.remove(sender.getNodeId());
        if (logStorageReader != null) {
            logStorageReader.close();
        }
    }

    private long senderNextIndex(RaftStorage raftStorage) {
        long lastLogIndex = raftStorage.getLastIndexedTerm().getIndex();
        return lastLogIndex + 1;
    }

    // must be optimized for CPU usage
    private Mono<Void> appendEntriesJob(Sender sender, RaftStorage raftStorage, Cluster cluster, RaftGroup raftGroup) {
        // TODO replace with Context
        AtomicBoolean fastTrack = new AtomicBoolean(true);
        return Flux.from(newEntriesProcessor)
            // latency when not on the fast track
            .sample(delayInterval)
            .onBackpressureDrop()
            .concatMap(i -> {
                return Mono.defer(() -> Mono.just(appendEntriesRequest(sender, cluster, raftGroup, raftStorage)))
                           .doOnNext(appendEntriesRequest -> fastTrack.set(fastTrack(raftGroup, appendEntriesRequest)))
                           .flatMap(appendEntriesRequest -> sender
                                .appendEntries(raftGroup, appendEntriesRequest)
                                .doOnNext(appendEntriesResponse -> {
                                    if (appendEntriesResponse.getTerm() > raftStorage.getTerm()) {
                                        raftGroup.convertToFollower(appendEntriesResponse.getTerm());
                                        fastTrack.set(false);
                                    } else {
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
                                            fastTrack.set(true);
                                            LOGGER.info("[Node {}][group {}] Decrease nextIndex for sender {}", cluster.getLocalNodeId(), raftGroup.getGroupName(), sender.getNodeId());
                                            long next = nextIndex.get(sender.getNodeId()) - 1;
                                            nextIndex.put(sender.getNodeId(), Math.max(next, 1));
                                        }
                                    }
                                })
                                .onErrorResume(throwable -> {
                                    LOGGER.error("Error while append entries job!!", throwable);
                                    return Mono.empty();
                                })
                           )
                           .repeatWhen(Repeat.onlyIf(repeatContext -> fastTrack.get())); // is internal subscription fast enough ?
                           // TODO retry
            })
            .then()
            .doOnError(throwable -> {
                LOGGER.error("[Node {}][group {}] Append entries internal error [{}]", cluster.getLocalNodeId(), raftGroup.getGroupName(), throwable.getMessage());
            });
    }

    // TODO should use more other factors e.g. raftGroup.hasClients()
    private boolean fastTrack(RaftGroup raftGroup, AppendEntriesRequest appendEntriesRequest) {
        return appendEntriesRequest.getEntriesCount() > 0 && // -> often means additional unnecessary iteration
                (lastNewEntry + 2 * delayInterval.toMillis() > System.currentTimeMillis());
    }

    private AppendEntriesRequest appendEntriesRequest(Sender sender, Cluster cluster, RaftGroup raftGroup, RaftStorage raftStorage) {
        long senderNextIndex = nextIndex.get(sender.getNodeId());
        BoundedLogStorageReader logStorageReader = logStorageReaders.get(sender.getNodeId());
        return appendEntriesRequest(senderNextIndex, cluster, raftGroup, raftStorage, logStorageReader);
    }

    private AppendEntriesRequest appendEntriesRequest(long senderNextIndex, Cluster cluster, RaftGroup raftGroup, RaftStorage raftStorage, BoundedLogStorageReader logStorageReader) {
        long prevLogIndex = senderNextIndex - 1;
        int prevLogTerm = raftStorage.getTermByIndex(prevLogIndex);

        // If last log index ≥ nextIndex for a follower: send
        // AppendEntries RPC with log entries starting at nextIndex
        AppendEntriesRequest.Builder builder = AppendEntriesRequest.newBuilder();
        long lastIndex = raftStorage.getLastIndexedTerm().getIndex();
        if (lastIndex >= senderNextIndex) {
            Iterable<ByteBuffer> entries = logStorageReader.getRawEntriesByIndex(senderNextIndex, lastIndex, MAX_BATCH_SIZE);
            entries.forEach(rawLogEntry -> builder.addEntries(ByteString.copyFrom(rawLogEntry))); // always copy as ByteString is immutable
        }

        return builder
                .setTerm(raftStorage.getTerm())
                .setLeaderId(cluster.getLocalNodeId())
                .setPrevLogIndex(prevLogIndex)
                .setPrevLogTerm(prevLogTerm)
                .setLeaderCommit(raftGroup.getCommitIndex())
                .build();
    }

}
