package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.storage.meta.Configuration;
import io.github.pmackowski.rsocket.raft.transport.Sender;
import io.github.pmackowski.rsocket.raft.transport.protobuf.*;
import io.rsocket.Payload;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface InternalRaftServer extends RaftServer {

    Mono<AppendEntriesResponse> onAppendEntries(AppendEntriesRequest appendEntries);

    Mono<PreVoteResponse> onPreRequestVote(PreVoteRequest preRequestVote);

    Mono<VoteResponse> onRequestVote(VoteRequest requestVote);

    Mono<Payload> onClientRequest(Payload payload);

    Flux<Payload> onClientRequests(Publisher<Payload> payloads);

    Configuration getCurrentConfiguration();

    void senderAvailable(Sender sender);

    void senderUnavailable(Sender sender);

}
