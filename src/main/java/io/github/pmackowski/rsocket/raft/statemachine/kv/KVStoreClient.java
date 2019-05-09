package io.github.pmackowski.rsocket.raft.statemachine.kv;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.pmackowski.rsocket.raft.RaftException;
import io.github.pmackowski.rsocket.raft.rpc.CommandRequest;
import io.github.pmackowski.rsocket.raft.utils.NettyUtils;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.ByteBufPayload;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class KVStoreClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(KVStoreClient.class);

    private List<RSocket> rSockets = new ArrayList<>();
    private List<Integer> clientPorts;
    private RSocket leader;

    public KVStoreClient(List<Integer> clientPorts) {
        this.clientPorts = clientPorts;
    }

    public void start() {
        boolean rSocketInitialized = false;
        Iterator<Integer> iterator = clientPorts.iterator();
        // or maybe ask for leader in first step
        while (iterator.hasNext() && !rSocketInitialized) {
            Integer next = iterator.next() + 20000;
            try {
                RSocket rSocket = RSocketFactory.connect()
                        .transport(TcpClientTransport.create(next))
                        .start()
                        .block();
                rSocketInitialized = true;
                rSockets.add(rSocket);
            } catch (Exception e) {
                LOGGER.warn("Server {} not available. Trying next one...", next, e);
            }
        }
        if (!rSocketInitialized) {
            LOGGER.error("Cluster is down!");
        } else {
            leader = rSockets.get(0);
        }
    }

    public Mono<String> put(String key, String value) {
        CommandRequest command = CommandRequest.newBuilder().setKey(key).setValue(value).setSetOperation(true).build();
        return leader.requestResponse(ByteBufPayload.create(command.toByteArray()))
                     .map(Payload::getMetadataUtf8);
    }

    public Mono<Boolean> compareAndSet(String key, String expectedValue, String setValue) {
        return Mono.empty();
    }

    public Mono<String> setIfEmpty(String key, String value) {
        return Mono.empty();
    }

    public Mono<String> get(String key) {
        CommandRequest command = CommandRequest.newBuilder().setKey(key).setSetOperation(false).buildPartial();
        return leader.requestResponse(ByteBufPayload.create(command.toByteArray()))
                .map(Payload::getMetadataUtf8);
    }

    public Mono<String> get(String key, LocalDateTime date) {
        return Mono.empty();
    }

    public Flux<String> getStream(String key, LocalDateTime timestamp) {
        return Flux.empty();
    }

    public Flux<String> getStream(String key) {
        CommandRequest command = CommandRequest.newBuilder().setKey(key).setSetOperation(false).buildPartial();
        return leader.requestStream(ByteBufPayload.create(command.toByteArray()))
                .map(Payload::getMetadataUtf8);
    }

    public Flux<KeyValue> put(Publisher<KeyValue> keyValueFlux) {
        Flux<Payload> payloads = Flux.from(keyValueFlux).map(kv -> CommandRequest.newBuilder().setKey(kv.getKey()).setValue(kv.getValue()).setSetOperation(true).buildPartial())
                .map(commandRequest -> ByteBufPayload.create(commandRequest.toByteArray()));

        return leader.requestChannel(payloads).map(payload -> {
            try {
                return CommandRequest.parseFrom(NettyUtils.toByteArray(payload.sliceData()));
            } catch (InvalidProtocolBufferException e) {
                throw new RaftException(e);
            }
        }).map(c -> new KeyValue(c.getKey(), c.getValue()));
    }
}
