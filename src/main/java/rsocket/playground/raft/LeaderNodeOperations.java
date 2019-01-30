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

    private static final Duration HEARTBEAT_TIMEOUT = Duration.ofMillis(100);

    @Override
    public void onInit(Node node) {
        disposable = sendAppendEntries(node).subscribe();
    }

    @Override
    public void onExit(Node node) {
        disposable.dispose();
    }

    @Override
    public Mono<AppendEntriesResponse> onAppendEntries(Node node, AppendEntriesRequest appendEntries) {
        return Mono.just(appendEntries)
                   .map(appendEntries1 -> new AppendEntriesResponse().term(node.getCurrentTerm()));
    }

    @Override
    public Mono<VoteResponse> onRequestVote(Node node, VoteRequest requestVote) {
        return Mono.just(requestVote)
                .map(requestVote1 -> {
                    boolean voteGranted = requestVote.getTerm() > node.getCurrentTerm();

                    return new VoteResponse()
                            .voteGranted(voteGranted)
                            .term(node.getCurrentTerm());
                });

    }

    private Mono<Void> sendAppendEntries(Node node) {
        AppendEntriesRequest appendEntries = new AppendEntriesRequest()
                .term(node.getCurrentTerm())
                .leaderId(node.nodeId);

        return node.senders
                .publishOn(Schedulers.newElastic("append-entries"))
                .flatMap(rSocket -> sendAppendEntries(rSocket, appendEntries))
                .doOnNext(appendEntriesResult -> {
                    if (appendEntriesResult.getTerm() > node.getCurrentTerm()) {
                        node.convertToFollower(appendEntriesResult.getTerm());
                    }
                })
                .then();
    }

    private Flux<AppendEntriesResponse> sendAppendEntries(RSocket rSocket, AppendEntriesRequest appendEntries) {
        Payload payload = ObjectPayload.create(appendEntries);
        Publisher<Payload> publisher = Flux.interval(HEARTBEAT_TIMEOUT).map(i -> payload);

        return rSocket.requestChannel(publisher).map(payload1 -> ObjectPayload.dataFromPayload(payload1, AppendEntriesResponse.class));
    }
}
