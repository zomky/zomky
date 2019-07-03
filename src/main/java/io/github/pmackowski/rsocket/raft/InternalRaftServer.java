package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.rpc.AppendEntriesRequest;
import io.github.pmackowski.rsocket.raft.rpc.AppendEntriesResponse;
import reactor.core.publisher.Mono;

public interface InternalRaftServer extends RaftServer {

    Mono<AppendEntriesResponse> onAppendEntries(AppendEntriesRequest appendEntries);

}
