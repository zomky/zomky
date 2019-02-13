package rsocket.playground.raft;

import io.rsocket.Payload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rsocket.playground.raft.storage.ZomkyStorage;
import rsocket.playground.raft.transport.ObjectPayload;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class LeaderNodeOperations implements NodeOperations {

    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderNodeOperations.class);

    private static final Duration HEARTBEAT_TIMEOUT = Duration.ofMillis(100);

    private Map<Integer, Disposable> senders = new HashMap<>();

    @Override
    public void onInit(Node node, ZomkyStorage zomkyStorage) {
        node.availableSenders().subscribe(sender -> {
            LOGGER.info("[Node {}] Sender available {}", node.nodeId, sender.getNodeId());
            senders.put(sender.getNodeId(), heartbeats(sender, zomkyStorage, node).subscribe());
        });

        node.onSenderAvailable(sender -> {
            LOGGER.info("[Node {}] Sender available {}", node.nodeId, sender.getNodeId());
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
                .map(i -> heartbeatRequest(node, zomkyStorage))
                .map(ObjectPayload::create);
        return sender.getRSocket().requestChannel(payload);
    }

    private AppendEntriesRequest heartbeatRequest(Node node, ZomkyStorage zomkyStorage) {
        return new AppendEntriesRequest()
                .term(zomkyStorage.getTerm())
                .leaderId(node.nodeId);
    }
}
