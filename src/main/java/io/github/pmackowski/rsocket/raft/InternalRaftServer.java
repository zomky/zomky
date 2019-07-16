package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.client.protobuf.InfoRequest;
import io.github.pmackowski.rsocket.raft.client.protobuf.InfoResponse;
import io.github.pmackowski.rsocket.raft.transport.Sender;
import io.github.pmackowski.rsocket.raft.transport.protobuf.*;
import io.rsocket.Payload;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface InternalRaftServer extends RaftServer {

    Mono<AppendEntriesResponse> onAppendEntries(String groupName, AppendEntriesRequest appendEntries);

    Mono<PreVoteResponse> onPreRequestVote(String groupName, PreVoteRequest preRequestVote);

    Mono<VoteResponse> onRequestVote(String groupName, VoteRequest requestVote);

    Mono<Payload> onClientRequest(Payload payload);

    Flux<Payload> onClientRequests(Publisher<Payload> payloads);

    void senderAvailable(Sender sender);

    void senderUnavailable(Sender sender);

    Mono<InfoResponse> onInfoRequest(InfoRequest infoRequest);
}
