package io.github.zomky.integration;

import io.github.zomky.ClientSocketAcceptor;
import io.github.zomky.Node;
import io.github.zomky.NodeStorage;
import io.github.zomky.RaftSocketAcceptor;
import io.github.zomky.gossip.Cluster;
import io.github.zomky.gossip.GossipOnPingDelay;
import io.github.zomky.gossip.GossipProtocol;
import io.github.zomky.gossip.protobuf.InitJoinRequest;
import io.github.zomky.gossip.protobuf.InitJoinResponse;
import io.github.zomky.raft.RaftProtocol;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.udp.UdpServer;

import java.time.Duration;

public class TestNodeFactory {

    public static TestNodeFactory.ServerNodeFactory receive() {
        return new TestNodeFactory.ServerNodeFactory();
    }

    public static class ServerNodeFactory {

        private static final Logger LOGGER = LoggerFactory.getLogger(Node.class);

        private NodeStorage nodeStorage;
        private String nodeName;
        private int port = 7000;
        private Integer joinPort;
        private boolean retryJoin;
        // gossip
        private GossipOnPingDelay onPingDelay = GossipOnPingDelay.NO_DELAY;
        private Duration baseProbeTimeout = Duration.ofMillis(500);
        private Duration baseProbeInterval = Duration.ofMillis(1000);
        private int subgroupSize = 3;
        private int maxGossips = 10;
        private float lambdaGossipSharedMultiplier = 1.5f;
        private float indirectDelayRatio = 0.3f;
        private float nackRatio = 0.6f;
        private int maxLocalHealthMultiplier = 8;

        public TestNodeFactory.ServerNodeFactory storage(NodeStorage nodeStorage) {
            this.nodeStorage = nodeStorage;
            return this;
        }

        public TestNodeFactory.ServerNodeFactory nodeName(String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public TestNodeFactory.ServerNodeFactory port(int port) {
            this.port = port;
            return this;
        }

        public TestNodeFactory.ServerNodeFactory join(Integer joinPort) {
            if (joinPort != null) {
                this.joinPort = joinPort;
                this.retryJoin = false;
            }
            return this;
        }

        public TestNodeFactory.ServerNodeFactory retryJoin(Integer joinPort) {
            if (joinPort != null) {
                this.joinPort = joinPort;
                this.retryJoin = true;
            }
            return this;
        }

        public TestNodeFactory.ServerNodeFactory onPingDelay(GossipOnPingDelay onPingDelay) {
            this.onPingDelay = onPingDelay;
            return this;
        }

        public TestNodeFactory.ServerNodeFactory baseProbeTimeout(Duration baseProbeTimeout) {
            this.baseProbeTimeout = baseProbeTimeout;
            return this;
        }

        public TestNodeFactory.ServerNodeFactory baseProbeInterval(Duration baseProbeInterval) {
            this.baseProbeInterval = baseProbeInterval;
            return this;
        }

        public TestNodeFactory.ServerNodeFactory subgroupSize(int subgroupSize) {
            this.subgroupSize = subgroupSize;
            return this;
        }

        public TestNodeFactory.ServerNodeFactory maxGossips(int maxGossips) {
            this.maxGossips = maxGossips;
            return this;
        }

        public TestNodeFactory.ServerNodeFactory lambdaGossipSharedMultiplier(float lambdaGossipSharedMultiplier) {
            this.lambdaGossipSharedMultiplier = lambdaGossipSharedMultiplier;
            return this;
        }

        public TestNodeFactory.ServerNodeFactory indirectDelayRatio(float indirectDelayRatio) {
            this.indirectDelayRatio = indirectDelayRatio;
            return this;
        }

        public TestNodeFactory.ServerNodeFactory nackRatio(float nackRatio) {
            this.nackRatio = nackRatio;
            return this;
        }

        public TestNodeFactory.ServerNodeFactory maxLocalHealthMultiplier(int maxGossips) {
            this.maxGossips = maxGossips;
            return this;
        }

        public Mono<Node> start() {
            return Mono.defer(() -> {
                int nodeId = port;
                GossipProtocol gossipProtocol = GossipProtocol.builder()
                        .nodeId(nodeId)
                        .onPingDelay(onPingDelay)
                        .baseProbeTimeout(baseProbeTimeout)
                        .baseProbeInterval(baseProbeInterval)
                        .subgroupSize(subgroupSize)
                        .maxGossips(maxGossips)
                        .lambdaGossipSharedMultiplier(lambdaGossipSharedMultiplier)
                        .indirectDelayRatio(indirectDelayRatio)
                        .nackRatio(nackRatio)
                        .maxLocalHealthMultiplier(maxLocalHealthMultiplier)
                        .build();

                RaftProtocol raftProtocol = new RaftProtocol(nodeId);

                Connection gossipReceiver = UdpServer.create()
                        .port(nodeId + 20000)
                        .handle(gossipProtocol::onPing)
                        .doOnUnbound(c -> {
                            LOGGER.error("[Node {}] Gossip server is down!", nodeId);
                        })
                        .bindNow(Duration.ofSeconds(1));

                CloseableChannel raftReceiver = RSocketFactory.receive()
                        .acceptor(new RaftSocketAcceptor(gossipProtocol, raftProtocol))
                        .transport(TcpServerTransport.create(nodeId))
                        .start()
                        .block();

                raftReceiver.onClose()
                        .doFinally(signalType -> LOGGER.warn("[Node {}] Raft onClose", nodeId))
                        .subscribe();

                CloseableChannel clientReceiver = RSocketFactory.receive()
                        .acceptor(new ClientSocketAcceptor(raftProtocol))
                        .transport(TcpServerTransport.create(nodeId + 10000))
                        .start()
                        .block();

                clientReceiver.onClose()
                        .doFinally(signalType -> LOGGER.warn("[Node {}] Client onClose", nodeId))
                        .subscribe();

                Node node = new Node() {
                    @Override
                    public int getNodeId() {
                        return nodeId;
                    }

                    @Override
                    public Cluster getCluster() {
                        return gossipProtocol.getCluster();
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
                    public Mono<Void> onClose() {
                        return null;
                    }

                    @Override
                    public void dispose() {
                        gossipReceiver.dispose();
                        raftReceiver.dispose();
                        clientReceiver.dispose();
                    }
                };

                return Mono.justOrEmpty(joinPort)
                        .flatMap(joinPort1 -> node.join(joinPort1, retryJoin))
                        .thenReturn(node)
                        .doOnNext(i -> {
                            gossipProtocol.start();
//                               senders.start();
//                               raftProtocol.start();
                        });
            });
        }

    }

}
