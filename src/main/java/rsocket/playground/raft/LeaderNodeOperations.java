package rsocket.playground.raft;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import rsocket.playground.raft.transport.ObjectPayload;

import java.time.Duration;

public class LeaderNodeOperations implements NodeOperations {

    private static final Duration HEARTBEAT_TIMEOUT = Duration.ofMillis(100);

    private DirectProcessor<Payload> processor;
    private FluxSink<Payload> sink;

    private Disposable disposable;
    private Disposable disposable2;

    public LeaderNodeOperations() {
        this.processor = DirectProcessor.create();
        this.sink = processor.sink(FluxSink.OverflowStrategy.BUFFER);
    }

    @Override
    public void onInit(Node node) {
        disposable = sendAppendEntries(node).subscribe();

        AppendEntriesRequest appendEntries = new AppendEntriesRequest()
                .term(node.getCurrentTerm())
                .leaderId(node.nodeId);

        disposable2 = Flux.interval(HEARTBEAT_TIMEOUT)
                .map(i -> ObjectPayload.create(appendEntries))
                .doOnNext(payload -> sink.next(payload))
                .subscribe();
    }

    @Override
    public void onExit(Node node) {
        disposable2.dispose();
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
                    long currentTerm = node.getCurrentTerm();
                    boolean voteGranted = requestVote.getTerm() > currentTerm;

                    return new VoteResponse()
                            .voteGranted(voteGranted)
                            .term(node.getCurrentTerm());
                });

    }

    private Mono<Void> sendAppendEntries(Node node) {
        return Flux.defer(node::availableClients)
                .flatMap(this::sendAppendEntries)
                .doOnNext(appendEntriesResult -> {
                    if (appendEntriesResult.getTerm() > node.getCurrentTerm()) {
                        node.convertToFollowerIfObsolete(appendEntriesResult.getTerm());
                    }
                })
                .then();
    }

    private Flux<AppendEntriesResponse> sendAppendEntries(RSocket rSocket) {
        return rSocket.requestChannel(processor).map(payload1 -> ObjectPayload.dataFromPayload(payload1, AppendEntriesResponse.class));
    }
}
