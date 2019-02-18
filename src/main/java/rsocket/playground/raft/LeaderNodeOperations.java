package rsocket.playground.raft;

import io.rsocket.Payload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rsocket.playground.raft.storage.ZomkyStorage;
import rsocket.playground.raft.transport.ObjectPayload;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LeaderNodeOperations implements NodeOperations {

    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderNodeOperations.class);

    private static final Duration HEARTBEAT_TIMEOUT = Duration.ofMillis(50);

    private Map<Integer, Disposable> senders = new HashMap<>();

    /**
     * Reinitialized after election,
     * index of the next log entry
     * to send to that server (initialized to leader
     * last log index + 1)
     */
    private Map<Integer, Long> nextIndex = new HashMap<>();

    /**
     * Reinitialized after election,
     * index of highest log entry
     * known to be replicated on server
     * (initialized to 0, increases monotonically)
     */
    private Map<Integer, Long> matchIndex = new HashMap<>();

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
                       return new AppendEntriesResponse().term(currentTerm).success(true);
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
        Flux<Payload> payload = Flux.interval(HEARTBEAT_TIMEOUT)
                .onBackpressureDrop()
                .map(i -> heartbeatRequest(sender, node, zomkyStorage))
                .map(ObjectPayload::create);
        return sender.getRSocket()
                     .requestChannel(payload)
                     .doOnNext(payload1 -> {
                         AppendEntriesResponse appendEntriesResponse = ObjectPayload.dataFromPayload(payload1, AppendEntriesResponse.class);
                         if (appendEntriesResponse.isSuccess()) {
                             // If successful: update nextIndex and matchIndex for follower (§5.3)
                             long lastLogIndex = zomkyStorage.getLast().getIndex();
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
                             nextIndex.put(sender.getNodeId(), nextIndex.get(sender.getNodeId()));
                         }
                     });
    }

    private AppendEntriesRequest heartbeatRequest(Sender sender, Node node, ZomkyStorage zomkyStorage) {
        List<byte[]> entries = new ArrayList<>();
        List<Integer> terms = new ArrayList<>();
        long senderIdxId = nextIndex.get(sender.getNodeId());
        // If last log index ≥ nextIndex for a follower: send
        // AppendEntries RPC with log entries starting at nextIndex
//        if (zomkyStorage.getLast().getIndex() >= senderIdxId) {
//            ByteBuffer entries2 = zomkyStorage.getEntriesByIndex(senderIdxId, zomkyStorage.getLast().getIndex());
//            senderIdxId = zomkyStorage.getLast().getIndex();
//        }
        while (zomkyStorage.getLast().getIndex() >= senderIdxId) {
            LOGGER.info("zomkyStorage.getLast().getIndex() {}",zomkyStorage.getLast().getIndex());
            LOGGER.info("senderIdxId {}", senderIdxId);
            ByteBuffer entryByIndex = zomkyStorage.getEntryByIndex(senderIdxId);
            entries.add(entryByIndex.array());
            int termByIndex = zomkyStorage.getTermByIndex(senderIdxId);
            terms.add(termByIndex);
            senderIdxId = senderIdxId + 1;
        }

        long prevLogIndex = zomkyStorage.getLast().getIndex() - entries.size();

        int prevLogTerm = zomkyStorage.getTermByIndex(prevLogIndex);

        return new AppendEntriesRequest()
                .term(zomkyStorage.getTerm())
                .leaderId(node.nodeId)
                .prevLogIndex(prevLogIndex)
                .prevLogTerm(prevLogTerm)
                .entries(entries)
                .terms(terms)
                .leaderCommit(node.getCommitIndex());
    }
}
