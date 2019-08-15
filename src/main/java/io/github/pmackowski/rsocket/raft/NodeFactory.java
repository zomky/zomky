package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.gossip.Cluster;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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

        public Mono<RSocket> start() {
            return RSocketFactory.connect()
                        .transport(TcpClientTransport.create(port + 10000))
                        .start();
        }
    }

    public static class ServerNodeFactory {

        private NodeStorage nodeStorage;
        private String nodeName;
        private Integer port;
        private Integer joinPort;
        private boolean retryJoin;
        private Cluster cluster;

        public ServerNodeFactory storage(NodeStorage nodeStorage) {
            this.nodeStorage = nodeStorage;
            return this;
        }

        public ServerNodeFactory nodeName(String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public ServerNodeFactory port(Integer port) {
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

        // only for testing purposes // TODO should be removed
        public ServerNodeFactory cluster(Cluster cluster) {
            this.cluster = cluster;
            return this;
        }

        public Mono<Node> start() {
            return Mono.defer(() -> {
                if (cluster == null) {
                    this.cluster = new Cluster(port);
                }
                DefaultNode node = new DefaultNode(nodeStorage, nodeName, port, cluster);
                node.startReceiver();

                return Mono.justOrEmpty(joinPort)
                           .flatMap(joinPort1 -> node.join(joinPort1, retryJoin))
                           .thenReturn(node)
                           .doOnNext(DefaultNode::start);
            });
        }

    }

}
