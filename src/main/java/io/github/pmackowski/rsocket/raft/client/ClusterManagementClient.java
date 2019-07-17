package io.github.pmackowski.rsocket.raft.client;

import io.github.pmackowski.rsocket.raft.storage.meta.Configuration;
import io.github.pmackowski.rsocket.raft.transport.Sender;
import io.github.pmackowski.rsocket.raft.transport.protobuf.AddServerRequest;
import io.github.pmackowski.rsocket.raft.transport.protobuf.AddServerResponse;
import io.github.pmackowski.rsocket.raft.transport.protobuf.CreateGroupRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.stream.Collectors;

public class ClusterManagementClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterManagementClient.class);

    public Mono<Void> createGroup(String groupName, int leaderId, Configuration configuration) {
        CreateGroupRequest createGroupRequest = CreateGroupRequest.newBuilder()
                .addAllNodes(configuration.getMembers())
                .setLeaderId(leaderId)
                .setGroupName(groupName)
                .build();
        List<Sender> senders = configuration.getMembers().stream().map(Sender::createSender).collect(Collectors.toList());

        return Flux.fromIterable(senders)
                .flatMap(sender -> sender.createGroup(groupName, createGroupRequest))
                .then();
    }

    public Mono<AddServerResponse> addServer(String groupName, int leaderId, int newServer) {
        Sender sender = Sender.createSender(leaderId);
        AddServerRequest addServerRequest = AddServerRequest.newBuilder().setNewServer(newServer).build();
        return sender.addServer(groupName, addServerRequest);
    }

    /*
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

     */
}
