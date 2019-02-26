package rsocket.playground.raft;

import io.rsocket.Payload;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.retry.Repeat;
import rsocket.playground.raft.storage.ZomkyStorage;
import rsocket.playground.raft.transport.ObjectPayload;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class LeaderNodeOperations implements NodeOperations {

    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderNodeOperations.class);

    private static final Duration HEARTBEAT_TIMEOUT = Duration.ofMillis(20);

    private ConcurrentMap<Integer, Disposable> senders = new ConcurrentHashMap<>();

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

    @Override
    public Mono<Payload> onClientRequest(Node node, ZomkyStorage zomkyStorage, Payload payload) {
        return Mono.just(payload)
                .doOnNext(content -> {
                    zomkyStorage.appendLog(zomkyStorage.getTerm(), payload.getData());

                    long lastLogIndex = zomkyStorage.getLast().getIndex();
                    while (node.getCommitIndex() < lastLogIndex) {
                        // NO-OP
                    }
                })
                .doOnNext(s -> LOGGER.info("Server received payload"));
    }

    @Override
    public Flux<Payload> onClientRequests(Node node, ZomkyStorage zomkyStorage, Publisher<Payload> payloads) {
        return new SenderConfirmOperator(Flux.from(payloads), node, zomkyStorage);
    }

    @Override
    public void onInit(Node node, ZomkyStorage zomkyStorage) {

        node.availableSenders().subscribe(sender -> {
            LOGGER.info("[Node {}] Sender available {}", node.nodeId, sender.getNodeId());
            long lastLogIndex = zomkyStorage.getLast().getIndex();
            long nextIdx = lastLogIndex + 1;
            nextIndex.put(sender.getNodeId(), nextIdx);
            matchIndex.put(sender.getNodeId(), 0L);
            senders.put(sender.getNodeId(), heartbeats(sender, zomkyStorage, node).subscribe());
        });

        node.onSenderAvailable(sender -> {
            LOGGER.info("[Node {}] Sender available {}", node.nodeId, sender.getNodeId());
            long lastLogIndex = zomkyStorage.getLast().getIndex();
            long nextIdx = lastLogIndex + 1;
            nextIndex.put(sender.getNodeId(), nextIdx);
            matchIndex.put(sender.getNodeId(), 0L);

            senders.put(sender.getNodeId(), heartbeats(sender, zomkyStorage, node).subscribe());
        });

        node.onSenderUnavailable(sender -> {
            LOGGER.info("[Node {}] Sender unavailable {}", node.nodeId, sender.getNodeId());
            Disposable disposable = senders.remove(sender.getNodeId());
            if (disposable != null) {
                disposable.dispose();
            }
        });
    }

    @Override
    public void onExit(Node node, ZomkyStorage zomkyStorage) {
        senders.values().forEach(Disposable::dispose);
    }

    @Override
    public Mono<AppendEntriesResponse> onAppendEntries(Node node, ZomkyStorage zomkyStorage, AppendEntriesRequest appendEntries) {
        return Mono.just(appendEntries)
                   .map(appendEntries1 -> {
                       int currentTerm = zomkyStorage.getTerm();
                       if (appendEntries1.getTerm() > currentTerm) {
                           node.convertToFollower(appendEntries1.getTerm());
                       }
                       return new AppendEntriesResponse().term(currentTerm).success(false);
                   });
    }

    @Override
    public Mono<VoteResponse> onRequestVote(Node node, ZomkyStorage zomkyStorage, VoteRequest requestVote) {
        return Mono.just(requestVote)
                .map(requestVote1 -> {
                    int currentTerm = zomkyStorage.getTerm();
                    boolean voteGranted = requestVote.getTerm() > currentTerm;
                    if (voteGranted) {
                        node.convertToFollower(requestVote.getTerm());
                    }
                    return new VoteResponse()
                            .voteGranted(voteGranted)
                            .term(currentTerm);
                });
    }

    private Flux<Payload> heartbeats(Sender sender, ZomkyStorage zomkyStorage, Node node) {
        return Mono.defer(() -> Mono.just(heartbeatRequest(sender, node, zomkyStorage)))
                   .flatMap(appendEntriesRequest -> sender.getAppendEntriesSocket()
                             .requestResponse(ObjectPayload.create(appendEntriesRequest))
                             .doOnNext(payload -> {
                                 AppendEntriesResponse appendEntriesResponse = ObjectPayload.dataFromPayload(payload, AppendEntriesResponse.class);
                                 if (appendEntriesResponse.isSuccess()) {
                                     // If successful: update nextIndex and matchIndex for follower (§5.3)
                                     long lastLogIndex = appendEntriesRequest.getPrevLogIndex() + appendEntriesRequest.getEntriesSize();
                                     long nextIdx = lastLogIndex + 1;
                                     nextIndex.put(sender.getNodeId(), nextIdx);
                                     matchIndex.put(sender.getNodeId(), lastLogIndex);

                                     // If there exists an N such that N > commitIndex, a majority
                                     // of matchIndex[i] ≥ N, and log[N].term == currentTerm:
                                     // set commitIndex = N (§5.3, §5.4).
                                     long candidateCommitIndex = node.getCommitIndex();
                                     while (candidateCommitIndex < lastLogIndex) {
                                         long i = candidateCommitIndex + 1;
                                         long count = matchIndex.values().stream().filter(matchIndex1 -> matchIndex1 >= i).count();
                                         if (count >= 1 && zomkyStorage.getTermByIndex( candidateCommitIndex + 1) == zomkyStorage.getTerm()) { // 1 means majority for 3 elements cluster
                                             candidateCommitIndex++;
                                         } else {
                                             break;
                                         }
                                     }
                                     if (candidateCommitIndex > node.getCommitIndex()) {
                                         node.setCommitIndex(candidateCommitIndex);
                                     }
                                 } else {
                                     // If AppendEntries fails because of log inconsistency decrement nextIndex and retry (§5.3)
                                     // TODO now retry is done in next heartbeat
                                     LOGGER.info("[Node {}] Decrease nextIndex for sender {}", node.nodeId, sender.getNodeId());
                                     nextIndex.put(sender.getNodeId(), nextIndex.get(sender.getNodeId()) - 1);
                                 }
                             })
            ).repeatWhen(Repeat.onlyIf(objectRepeatContext -> true).fixedBackoff(HEARTBEAT_TIMEOUT));
    }

    private AppendEntriesRequest heartbeatRequest(Sender sender, Node node, ZomkyStorage zomkyStorage) {
        long senderIdxId = nextIndex.get(sender.getNodeId());
        // If last log index ≥ nextIndex for a follower: send
        // AppendEntries RPC with log entries starting at nextIndex
        ByteBuffer entries = null;

        long lastIndex = zomkyStorage.getLast().getIndex();
        if (lastIndex >= senderIdxId) {
            entries = zomkyStorage.getEntriesByIndex(senderIdxId, lastIndex);
        }
        long prevLogIndex = senderIdxId - 1;
        int prevLogTerm = zomkyStorage.getTermByIndex(prevLogIndex);

        return new AppendEntriesRequest()
                .term(zomkyStorage.getTerm())
                .leaderId(node.nodeId)
                .prevLogIndex(prevLogIndex)
                .prevLogTerm(prevLogTerm)
                .entries(entries != null ? entries.array() : null)
                .entriesSize((int) (lastIndex - senderIdxId + 1))
                .leaderCommit(node.getCommitIndex());
    }
}
