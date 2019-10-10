package io.github.zomky;

import io.github.zomky.gossip.GossipProtocol;
import io.github.zomky.gossip.protobuf.InitJoinRequest;
import io.github.zomky.gossip.protobuf.InitJoinResponse;
import io.github.zomky.metrics.MetricsCollector;
import io.github.zomky.metrics.NoMetricsCollector;
import io.github.zomky.raft.RaftProtocol;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.udp.UdpServer;

import java.time.Duration;

public class NodeFactory {

    public static ClientNodeFactory connect() {
        return new ClientNodeFactory();
    }

    public static ClientNodeFactory connectGroup(String groupName) {
        return new ClientNodeFactory();
    }

    public static ServerNodeFactory receive() {
        return new ServerNodeFactory();
    }

    public static class ClientNodeFactory {

        private int port;

        public ClientNodeFactory port(int port) {
            this.port = port;
            return this;
        }

        public Mono<ZomkyClient> start() {
            return RSocketFactory.connect()
                    .transport(TcpClientTransport.create(port))
                    .start()
                    .map(ZomkyClient::new)
                    .doOnNext(ZomkyClient::start);
        }

    }

    public static class ServerNodeFactory {

        private static final Logger LOGGER = LoggerFactory.getLogger(Node.class);

        private MetricsCollector metrics = new NoMetricsCollector();
        private NodeStorage nodeStorage;
        private String nodeName;
        private int port = 7000;
        private Integer joinPort;
        private boolean retryJoin;
        // gossip
        private Duration baseProbeTimeout = Duration.ofMillis(500);
        private Duration baseProbeInterval = Duration.ofMillis(2000);
        private int subgroupSize = 3;
        private int maxGossips = 10;
        private float lambdaGossipSharedMultiplier = 1.5f;
        private float indirectDelayRatio = 0.3f;
        private float nackRatio = 0.6f;
        private int maxLocalHealthMultiplier = 8;

        public ServerNodeFactory metrics(MetricsCollector metrics) {
            this.metrics = metrics;
            return this;
        }

        public ServerNodeFactory storage(NodeStorage nodeStorage) {
            this.nodeStorage = nodeStorage;
            return this;
        }

        public ServerNodeFactory nodeName(String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public ServerNodeFactory port(int port) {
            this.port = port;
            return this;
        }

        public ServerNodeFactory join(Integer joinPort) {
            if (joinPort != null) {
                this.joinPort = joinPort;
                this.retryJoin = false;
            }
            return this;
        }

        public ServerNodeFactory retryJoin(Integer joinPort) {
            if (joinPort != null) {
                this.joinPort = joinPort;
                this.retryJoin = true;
            }
            return this;
        }

        public ServerNodeFactory baseProbeTimeout(Duration baseProbeTimeout) {
            this.baseProbeTimeout = baseProbeTimeout;
            return this;
        }

        public ServerNodeFactory baseProbeInterval(Duration baseProbeInterval) {
            this.baseProbeInterval = baseProbeInterval;
            return this;
        }

        public ServerNodeFactory subgroupSize(int subgroupSize) {
            this.subgroupSize = subgroupSize;
            return this;
        }

        public ServerNodeFactory maxGossips(int maxGossips) {
            this.maxGossips = maxGossips;
            return this;
        }

        public ServerNodeFactory lambdaGossipSharedMultiplier(float lambdaGossipSharedMultiplier) {
            this.lambdaGossipSharedMultiplier = lambdaGossipSharedMultiplier;
            return this;
        }

        public ServerNodeFactory indirectDelayRatio(float indirectDelayRatio) {
            this.indirectDelayRatio = indirectDelayRatio;
            return this;
        }

        public ServerNodeFactory nackRatio(float nackRatio) {
            this.nackRatio = nackRatio;
            return this;
        }

        public ServerNodeFactory maxLocalHealthMultiplier(int maxGossips) {
            this.maxGossips = maxGossips;
            return this;
        }

        public Mono<Node> start() {
            return Mono.defer(() -> {
                int nodeId = port;
                Cluster cluster = new Cluster(nodeId);
                GossipProtocol gossipProtocol = GossipProtocol.builder()
                        .nodeId(nodeId)
                        .baseProbeTimeout(baseProbeTimeout)
                        .baseProbeInterval(baseProbeInterval)
                        .subgroupSize(subgroupSize)
                        .maxGossips(maxGossips)
                        .lambdaGossipSharedMultiplier(lambdaGossipSharedMultiplier)
                        .indirectDelayRatio(indirectDelayRatio)
                        .nackRatio(nackRatio)
                        .maxLocalHealthMultiplier(maxLocalHealthMultiplier)
                        .build();

                gossipProtocol.nodeAvailable().subscribe(cluster::addMember);
                gossipProtocol.nodeUnavailable().subscribe(cluster::removeMember);

                RaftProtocol raftProtocol = new RaftProtocol(cluster, metrics);

                Connection gossipReceiver = UdpServer.create()
                        .port(nodeId + 20000)
                        .handle(gossipProtocol::onPing)
                        .doOnUnbound(c -> {
                            LOGGER.error("[Node {}] Gossip server is down!", nodeId);
                        })
                        .bindNow(Duration.ofSeconds(1));

                CloseableChannel nodeReceiver = RSocketFactory.receive()
                        .acceptor(new NodeSocketAcceptor(gossipProtocol, raftProtocol))
                        .transport(TcpServerTransport.create(nodeId))
                        .start()
                        .block();

                nodeReceiver.onClose()
                        .doFinally(signalType -> LOGGER.warn("[Node {}] Node onClose", nodeId))
                        .subscribe();

                Node node = new Node() {
                    @Override
                    public int getNodeId() {
                        return nodeId;
                    }

                    @Override
                    public Cluster getCluster() {
                        return cluster;
                    }

                    @Override
                    public Mono<InitJoinResponse> join(Integer joinPort, boolean retry) {
                        return gossipProtocol.join(InitJoinRequest.newBuilder()
                                .setRequesterPort(nodeId)
                                .setPort(joinPort)
                                .setRetry(retry)
                                .build());
                    }

                    @Override
                    public RaftProtocol getRaftProtocol() {
                        return raftProtocol;
                    }

                    @Override
                    public Mono<Void> onClose() {
                        return null;
                    }

                    @Override
                    public void dispose() {
                        gossipReceiver.dispose();
                        nodeReceiver.dispose();
                    }
                };

                return Mono.justOrEmpty(joinPort)
                        .flatMap(joinPort1 -> node.join(joinPort1, retryJoin))
                        .thenReturn(node)
                        .doOnNext(i -> {
                            raftProtocol.start();
                            gossipProtocol.start();
                        });
            });
        }


    }
}
