package rsocket.playground.raft;

import io.rsocket.util.DefaultPayload;
import org.junit.Test;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.retry.Repeat;
import rsocket.playground.raft.h2.H2RepositoryImpl;

import java.time.Duration;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static rsocket.playground.raft.NodeTest.TestRSocket.failure;
import static rsocket.playground.raft.NodeTest.TestRSocket.ok;

public class NodeTest {

    private Disposable dupa;

    private static final Logger LOGGER = LoggerFactory.getLogger(NodeTest.class);

    static class TestRSocket {

        private boolean voted;
        private long durationInSeconds;
        private boolean failure;

        public static TestRSocket ok(boolean voted, long durationInSeconds) {
            return new TestRSocket(voted, durationInSeconds, false);
        }

        public static TestRSocket failure(long durationInSeconds) {
            return new TestRSocket(false, durationInSeconds, true);
        }

        public TestRSocket(boolean voted, long durationInSeconds, boolean failure) {
        this.voted = voted;
        this.durationInSeconds = durationInSeconds;
        this.failure = failure;
    }

    Mono<Boolean> requestResponse() {
        if (failure) {
            return Mono.just(voted).delayElement(Duration.ofSeconds(durationInSeconds))
                    .doOnNext(e -> {
                        throw new RuntimeException("error socket");
                    });
        } else {
            return Mono.just(voted).delayElement(Duration.ofSeconds(durationInSeconds));
        }
    }
}

    @Test
    public void name2() throws InterruptedException {
        Flux<TestRSocket> sockets = Flux.just(
                failure(1),
                ok(true, 1),
                ok(false, 2),
                ok(true, 4)
        );

        int quorum = 2;
        Duration ELECTION_TIMEOUT = Duration.ofSeconds(2);

        AtomicBoolean repeat = new AtomicBoolean(false);

        Mono.defer(() -> mono(sockets, repeat))
                .repeatWhen(Repeat
                        .onlyIf(repeatContext -> {
                            AtomicBoolean repeat2 = (AtomicBoolean) repeatContext.applicationContext();
                            return repeat2.get();
                        })
                        .withApplicationContext(repeat)
                        .doOnRepeat(objectRepeatContext -> {
                            LOGGER.info("repeating, new election is required");
                        }))
                .subscribe();

        Thread.sleep(100000);
    }

    private Mono<Void> mono(Flux<TestRSocket> sockets, AtomicBoolean repeat) {
        int quorum = 2;
        Random random = new Random();
        int e = random.nextInt(5) + 1;
        System.out.println("Election timeout " + e);
        Duration ELECTION_TIMEOUT = Duration.ofSeconds(e);

        return sockets.publishOn(Schedulers.newElastic("dupa"))
                .flatMap(socket ->
                        socket.requestResponse().onErrorReturn(false)
                )
                .filter(Boolean::booleanValue)
                .log()
                // wait until quorum achieved or election timeout elapsed
                .buffer(quorum)
                .timeout(ELECTION_TIMEOUT)
                .onErrorResume(s -> {
                    repeat.set(true);
                    return Mono.empty();
                })
                .next()
                .doOnNext(s -> {
                    repeat.set(false);
                })
                .log()
                .then();
    }

    @Test
    public void name() throws InterruptedException {
        NodeRepository nodeRepository = new H2RepositoryImpl();

        Node node1 = Node.create(7000, nodeRepository, Arrays.asList(7001, 7002));
        Node node2 = Node.create(7001, nodeRepository, Arrays.asList(7000, 7002));
        Node node3 = Node.create(7002, nodeRepository, Arrays.asList(7000, 7001));

        node1.start();
        node2.start();
        node3.start();

        Thread.sleep(6000);

        Client client = new Client(Arrays.asList(7000,7001));
        client.start();

        client.send(DefaultPayload.create("Abc"))
              .subscribe(s -> LOGGER.info("Client received {}", s.getDataUtf8()));

        Thread.sleep(1000000);
    }

}