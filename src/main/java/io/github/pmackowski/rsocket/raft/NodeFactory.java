package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.gossip.Cluster;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
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

        private int port;
        private Cluster cluster;

        public ServerNodeFactory port(int port) {
            this.port = port;
            return this;
        }

        public ServerNodeFactory cluster(Cluster cluster) {
            this.cluster = cluster;
            return this;
        }

        public Mono<Node> start() {
            return Mono.defer(() -> {
                DefaultNode kvStoreServer = new DefaultNode(port, cluster);
                return Mono.just(kvStoreServer).doOnNext(DefaultNode::start);
            });
        }

    }

}
