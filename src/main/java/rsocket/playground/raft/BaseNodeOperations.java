package rsocket.playground.raft;

import io.rsocket.Payload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import rsocket.playground.raft.transport.ObjectPayload;

public class BaseNodeOperations implements NodeOperations {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseNodeOperations.class);

    protected DirectProcessor<Payload> processor;
    protected FluxSink<Payload> sink;
    protected Disposable disposable;

    public BaseNodeOperations() {
        this.processor = DirectProcessor.create();
        this.sink = processor.sink(FluxSink.OverflowStrategy.BUFFER);
    }

    @Override
    public void onInit(Node node) {
        throw new RaftException("onInit not implemented!");
    }

    @Override
    public void onExit(Node node) {
        throw new RaftException("onExit not implemented!");
    }

    @Override
    public Mono<AppendEntriesResponse> onAppendEntries(Node node, AppendEntriesRequest appendEntries) {
        throw new RaftException("onAppendEntries not implemented!");
    }

    @Override
    public Mono<VoteResponse> onRequestVote(Node node, VoteRequest requestVote) {
        throw new RaftException("onRequestVote not implemented!");
    }

    @Override
    public Mono<Payload> onPayloadRequest(Node node, Payload payload) {
        sink.next(payload);
        TermAware termAware = ObjectPayload.dataFromPayload(payload, TermAware.class);
        node.convertToFollower(termAware.getTerm());
        //LOGGER.info("Node {} received message {}", node.nodeId, termAware);
        return Mono.just(payload);
    }

}
