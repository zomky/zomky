package io.github.pmackowski.rsocket.raft.client;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.pmackowski.rsocket.raft.raft.RaftException;
import io.github.pmackowski.rsocket.raft.client.protobuf.InfoRequest;
import io.github.pmackowski.rsocket.raft.client.protobuf.InfoResponse;
import io.github.pmackowski.rsocket.raft.raft.RaftGroup;
import io.github.pmackowski.rsocket.raft.transport.RpcType;
import io.github.pmackowski.rsocket.raft.transport.Sender;
import io.github.pmackowski.rsocket.raft.transport.protobuf.AddServerRequest;
import io.github.pmackowski.rsocket.raft.transport.protobuf.AddServerResponse;
import io.github.pmackowski.rsocket.raft.transport.protobuf.RemoveServerRequest;
import io.github.pmackowski.rsocket.raft.utils.NettyUtils;
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
    private Sender sender;

    public ClusterManagementClient(int leaderId) {
        this.sender = Sender.createSender(leaderId);
//        this.leaderMono = RSocketFactory.connect()
//                .transport(TcpClientTransport.create(leaderId))
//                .start()
//                .cache();
    }

    public Mono<AddServerResponse> addServer(String groupName, int nodeId) {
        AddServerRequest addServerRequest = AddServerRequest.newBuilder().setNewServer(7001).build();
        return sender.addServer(groupName, addServerRequest);
    }

    public Mono<AddServerResponse> addServer(String groupName, AddServerRequest addServerRequest) {
        return sender.addServer(groupName, addServerRequest);
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

    public Mono<InfoResponse> clusterInfo() {
        return leaderMono.flatMap(leader -> {
            InfoRequest infoRequest = InfoRequest.newBuilder().build();
            Payload payload = ByteBufPayload.create(infoRequest.toByteArray(), new byte[] {RpcType.INFO.getCode()});
            return leader.requestResponse(payload);
        }).map(payload1 -> {
            try {
                return InfoResponse.parseFrom(NettyUtils.toByteArray(payload1.sliceData()));
            } catch (InvalidProtocolBufferException e) {
                throw new RaftException("Invalid info response!", e);
            }
        });
    }
}
