package rsocket.playground.raft;

import io.rsocket.Payload;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

public class BaseNodeOperations implements NodeOperations {

    protected DirectProcessor<Payload> processor;
    protected FluxSink<Payload> sink;
    protected Disposable disposable;

    public BaseNodeOperations() {
        this.processor = DirectProcessor.create();
        this.sink = processor.sink(FluxSink.OverflowStrategy.BUFFER);
    }

    @Override
    public void onInit(Node node) {

    }

    @Override
    public void onExit(Node node) {

    }

    @Override
    public Mono<AppendEntriesResult> onAppendEntries(Node node, AppendEntries appendEntries) {
        return null;
    }

    @Override
    public Mono<RequestVoteResult> onRequestVote(Node node, RequestVote requestVote) {
        return null;
    }

    @Override
    public Mono<Payload> onPayloadRequest(Payload payload) {
        sink.next(payload);
        return Mono.just(payload);
    }
}
