package io.github.zomky;

import io.github.zomky.raft.StateMachineEntryConverter;
import io.github.zomky.raft.StateMachineUtils;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.DefaultPayload;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

// TODO autorecovery
public class ZomkyClient {

    private static final Duration SESSION_INTERVAL = Duration.ofSeconds(1);
    private static String DEFAULT_SUBSYSTEM = "default.subsystem";
    private static String DEFAULT_GROUP = "default.group";
    private static int DEFAULT_GROUP_SIZE = 3;

    private RSocket rSocket;
    private final UUID clientId = UUID.randomUUID();
    private final Disposable.Composite actionsDisposables = Disposables.composite();
    private final Map<String, StateMachineEntryConverter> converters;

    public ZomkyClient(RSocket rSocket) {
        this.rSocket = rSocket;
        this.converters = StateMachineUtils.stateMachineConverters();
    }

    public void start() {
        // maintains session with server
        actionsDisposables.add(Flux.interval(Duration.ZERO, SESSION_INTERVAL)
            .then(rSocket.metadataPush(DefaultPayload.create(clientId.toString())))
            .subscribe());
    }

    public void close() {
        actionsDisposables.dispose();
    }

    public Mono<Void> addCPSubsystem(String name, int size) {
        return Mono.error(new NotImplementedException());
    }

    public Mono<Void> addGroup(String name) {
        return addGroup(name, DEFAULT_SUBSYSTEM, DEFAULT_GROUP_SIZE);
    }

    public Mono<Void> addGroup(String name, String subsystem, int groupSize) {
        return Mono.error(new NotImplementedException());
    }

    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
        return requestChannel(DEFAULT_GROUP, payloads);
    }

    public Flux<Payload> requestChannel(String group, Publisher<Payload> payloads) {
        return Flux.error(new NotImplementedException());
    }

}
