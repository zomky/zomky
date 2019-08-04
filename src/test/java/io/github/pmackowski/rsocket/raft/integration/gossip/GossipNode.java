package io.github.pmackowski.rsocket.raft.integration.gossip;

import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ack;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Gossip;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ping;
import io.netty.channel.socket.DatagramPacket;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.udp.UdpInbound;
import reactor.netty.udp.UdpOutbound;
import reactor.netty.udp.UdpServer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.github.pmackowski.rsocket.raft.integration.gossip.PingUtils.toPing;

public class GossipNode {

    private static final Logger LOGGER = LoggerFactory.getLogger(GossipNode.class);

    private long probeTimeout = 500;
    private long protocolPeriod = probeTimeout * 3;

    private AtomicLong protocolPeriodCounter = new AtomicLong(0);
    private GossipOnPingDelay onPingDelay;

    private List<Integer> nodes = new ArrayList<>();

    int nodeId;
    private Gossips gossips;
    private Connection connection;
    private Disposable disposable;

    public GossipNode(int nodeId) {
        this(nodeId, GossipOnPingDelay.NO_DELAY);
    }

    public GossipNode(int nodeId, GossipOnPingDelay onPingDelay) {
        this.nodeId = nodeId;
        this.onPingDelay = onPingDelay;
        this.gossips = new Gossips(nodeId);
        this.connection = UdpServer.create()
                .port(nodeId)
                .handle(this::onPing)
                .bindNow(Duration.ofSeconds(1));
    }

    public void disposeNow() {
        connection.disposeNow();
        disposable.dispose();
    }

    public void start() {
//        Queue<Integer> nodesInRandomOrder = new ArrayList<>();
//        int selectedPeer = nodesInRandomOrder.poll();

        GossipProbe gossipProbe = new GossipProbe(this);

        disposable = Flux.defer(() -> {
                    protocolPeriodCounter.incrementAndGet();
                    int destinationNodeId = 7001; // round-robin from random list
                    List<Integer> proxyNodeIds = Arrays.asList(7002, 7003);
                    List<Gossip> gossips = new ArrayList<>();
                    Publisher<?> indirectStart = Mono.delay(Duration.ofMillis(400));
                    Publisher<?> protocolPeriodEnd = Mono.delay(Duration.ofSeconds(2));
                    return gossipProbe.probeNode(destinationNodeId, proxyNodeIds, gossips, indirectStart, protocolPeriodEnd);
                })
                .doOnNext(ack -> {
                    this.gossips.addGossips(ack);
                })
                .doOnError(throwable -> {
//                    this.gossips.addGossip(destinationNodeId, Gossip.Suspicion.SUSPECT);
                })
                .doFinally(signalType -> {
        //                Gossip.Suspicion suspicion = acks.get() > 0 ? Gossip.Suspicion.ALIVE : Gossip.Suspicion.SUSPECT;
        //                this.gossips.addGossip(nodeId, suspicion);
                    LOGGER.info("[Node {}][ping] Probing {} finished.", this.nodeId, 2313123);
                })
                .repeat()
                .subscribe();

        List<Integer> indirectPeers = new ArrayList<>(); // select configurable amount of peers
    }

    Publisher<Void> onPing(UdpInbound udpInbound, UdpOutbound udpOutbound) {
        return udpInbound.receiveObject()
                .cast(DatagramPacket.class)
                .flatMap(datagramPacket -> {
                    Ping ping = toPing(datagramPacket);
                    log(ping);
                    List<Gossip> sharedGossips = gossips.mergeAndShare(ping.getGossipsList());
                    Ack ack = Ack.newBuilder().setNodeId(nodeId).addAllGossips(sharedGossips).build();

                    Publisher<?> publisher;
                    if (ping.getDirect()) {
                        publisher = onPingDelay.apply(ping.getRequestorNodeId(), protocolPeriodCounter.get());
                    } else {
                        GossipTransport gossipTransport = new GossipTransport();
                        Ping newPing = PingUtils.direct(ping, sharedGossips);
                        publisher = gossipTransport.ping(newPing); // TODO handle ack and timeout
                    }
                    return Flux.from(publisher).then(udpOutbound.sendObject(AckUtils.toDatagram(ack, datagramPacket.sender())).then());
                })
                .then();
    }

    private void log(Ping ping) {
        if (ping.getDirect()) {
            if (ping.getInitiatorNodeId() == ping.getRequestorNodeId()) {
                LOGGER.info("[Node {}][onPing] I am being probed by {}", ping.getDestinationNodeId(), ping.getRequestorNodeId());
            } else {
                LOGGER.info("[Node {}][onPing] I am being probed by {} on behalf of {}", ping.getDestinationNodeId(), ping.getRequestorNodeId(), ping.getInitiatorNodeId());
            }
        }
    }

}
