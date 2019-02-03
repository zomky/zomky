package rsocket.playground.raft;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.retry.Repeat;
import rsocket.playground.raft.transport.ObjectPayload;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LeaderNodeOperations implements NodeOperations {

    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderNodeOperations.class);

    private static final Duration HEARTBEAT_TIMEOUT = Duration.ofMillis(100);

    private Map<Integer, Disposable> senders = new HashMap<>();

    @Override
    public void onInit(Node node) {
        node.availableSenders().subscribe(sender -> {
            LOGGER.info("sender available {}", sender.getNodeId());
            senders.put(sender.getNodeId(), heartbeats(sender, node).subscribe());
        });

        node.onSenderAvailable(sender -> {
            LOGGER.info("sender available {}", sender.getNodeId());
            senders.put(sender.getNodeId(), heartbeats(sender, node).subscribe());
        });

        node.onSenderUnavailable(sender -> {
            LOGGER.info("sender unavailable {}", sender.getNodeId());
            Disposable disposable = senders.remove(sender.getNodeId());
            if (disposable != null) {
                disposable.dispose();
            }
        });
    }

    @Override
    public void onExit(Node node) {
        senders.values().forEach(Disposable::dispose);
    }

    @Override
    public Mono<AppendEntriesResponse> onAppendEntries(Node node, AppendEntriesRequest appendEntries) {
        return Mono.just(appendEntries)
                   .map(appendEntries1 -> {
                       long currentTerm = node.getCurrentTerm();
                       if (appendEntries1.getTerm() > currentTerm) {
                           node.convertToFollower(appendEntries1.getTerm());
                       }
                       return new AppendEntriesResponse().term(currentTerm).success(true);
                   });
    }

    @Override
    public Mono<VoteResponse> onRequestVote(Node node, VoteRequest requestVote) {
        return Mono.just(requestVote)
                .map(requestVote1 -> {
                    long currentTerm = node.getCurrentTerm();
                    boolean voteGranted = requestVote.getTerm() > currentTerm;
                    if (voteGranted) {
                        node.convertToFollower(requestVote.getTerm());
                    }
                    return new VoteResponse()
                            .voteGranted(voteGranted)
                            .term(currentTerm);
                });

    }

    private Flux<Payload> heartbeats(Sender sender, Node node) {
        Flux<Payload> payload = Flux.interval(HEARTBEAT_TIMEOUT)
                .map(i -> heartbeatRequest(node))
                .map(ObjectPayload::create);
        return sender.getRSocket().requestChannel(payload);
    }

    private AppendEntriesRequest heartbeatRequest(Node node) {
        return new AppendEntriesRequest()
                .term(node.getCurrentTerm())
                .leaderId(node.nodeId);
    }
}
