package io.github.zomky.external.statemachine;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.zomky.external.protobuf.CommandRequest;
import io.github.zomky.raft.RaftException;
import io.github.zomky.utils.NettyUtils;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.ByteBufPayload;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class KVStoreClient {

    private Mono<RSocket> leaderMono;

    public KVStoreClient(int leaderId) {
        this.leaderMono = RSocketFactory.connect()
                .transport(TcpClientTransport.create(leaderId + 10000))
                .start();
    }

    public Flux<KeyValue> put(String group, Publisher<KeyValue> keyValueFlux) {
        Flux<Payload> payloads = Flux.from(keyValueFlux).map(kv -> CommandRequest.newBuilder().setKey(kv.getKey()).setValue(kv.getValue()).setSetOperation(true).buildPartial())
                .map(commandRequest -> ByteBufPayload.create(commandRequest.toByteArray(), group.getBytes()));

        return leaderMono.flatMapMany(leader -> leader.requestChannel(payloads))
                         .map(payload -> {
                            try {
                                return CommandRequest.parseFrom(NettyUtils.toByteArray(payload.sliceData()));
                            } catch (InvalidProtocolBufferException e) {
                                throw new RaftException(e);
                            }
                         })
                         .map(c -> new KeyValue(c.getKey(), c.getValue()));
    }
}
