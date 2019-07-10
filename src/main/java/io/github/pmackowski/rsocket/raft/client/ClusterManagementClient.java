package io.github.pmackowski.rsocket.raft.client;

import io.github.pmackowski.rsocket.raft.transport.RpcType;
import io.github.pmackowski.rsocket.raft.transport.protobuf.AddServerRequest;
import io.github.pmackowski.rsocket.raft.transport.protobuf.RemoveServerRequest;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.ByteBufPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class ClusterManagementClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterManagementClient.class);

    private Mono<RSocket> leaderMono;

    public ClusterManagementClient(int leaderId) {
        this.leaderMono = RSocketFactory.connect()
                .transport(TcpClientTransport.create(leaderId))
                .start()
                .cache();
    }

    public Mono<Boolean> addServer(int newServer) {
        return leaderMono.flatMap(leader -> {
            AddServerRequest addServerRequest = AddServerRequest.newBuilder().setNewServer(newServer).build();
            Payload payload = ByteBufPayload.create(addServerRequest.toByteArray(), new byte[] {RpcType.ADD_SERVER.getCode()});
            return leader.requestResponse(payload);
        }).map(payload -> true);
    }

    public Mono<Boolean> removeServer(int oldServer) {
        return leaderMono.flatMap(leader -> {
            RemoveServerRequest removeServerRequest = RemoveServerRequest.newBuilder().setOldServer(oldServer).build();
            Payload payload = ByteBufPayload.create(removeServerRequest.toByteArray(), new byte[] {RpcType.REMOVE_SERVER.getCode()});
            return leader.requestResponse(payload);
        }).map(payload -> true);
    }


}
