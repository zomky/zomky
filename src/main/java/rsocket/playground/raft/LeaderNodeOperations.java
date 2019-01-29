package rsocket.playground.raft;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import rsocket.playground.raft.transport.ObjectPayload;

import java.time.Duration;

public class LeaderNodeOperations extends BaseNodeOperations {

    private static final Duration HEARTBEAT_TIMEOUT = Duration.ofMillis(50);

    @Override
    public void onInit(Node node) {
        sendAppendEntries(node).subscribe();
    }

    @Override
    public void onExit(Node node) {

    }

    @Override
    public Mono<AppendEntriesResult> onAppendEntries(Node node, AppendEntries appendEntries) {
        throw new RaftException("leader does not support onAppendEntries!");
    }

    @Override
    public Mono<RequestVoteResult> onRequestVote(Node node, RequestVote requestVote) {
        return null;
    }

    private Mono<Void> sendAppendEntries(Node node) {
        AppendEntries appendEntries = new AppendEntries()
                .term(node.getCurrentTerm())
                .leaderId(node.nodeId);

        return node.senders
                .publishOn(Schedulers.newElastic("append-entries"))
                .flatMap(rSocket -> sendAppendEntries(rSocket, appendEntries))
                // if appendEntriesResult#term > current term T, set currentTerm = T and convert to follower
                .then();
    }

    private Flux<AppendEntriesResult> sendAppendEntries(RSocket rSocket, AppendEntries appendEntries) {
        Payload payload = ObjectPayload.create(appendEntries);
        Publisher<Payload> publisher = Flux.interval(HEARTBEAT_TIMEOUT).map(i -> payload);

        return rSocket.requestChannel(publisher).map(payload1 -> ObjectPayload.dataFromPayload(payload1, AppendEntriesResult.class));
    }
}
