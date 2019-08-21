package io.github.zomky.client;

import io.github.zomky.transport.Sender;
import io.github.zomky.transport.protobuf.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.stream.Collectors;

public class RaftManagementClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftManagementClient.class);

    public Mono<Void> addGroup(String groupName, AddGroupRequest addGroupRequest) {
        List<Sender> senders = addGroupRequest.getNodesList().stream().map(Sender::createSender).collect(Collectors.toList());

        return Flux.fromIterable(senders)
                .flatMap(sender -> sender.addGroup(groupName, addGroupRequest))
                .then();
    }

    public Mono<AddServerResponse> addServer(String groupName, int leaderId, int newServer) {
        Sender sender = Sender.createSender(leaderId);
        AddServerRequest addServerRequest = AddServerRequest.newBuilder().setNewServer(newServer).build();
        return sender.addServer(groupName, addServerRequest);
    }

    public Mono<RemoveServerResponse> removeServer(String groupName, int leaderId, int oldServer) {
        Sender sender = Sender.createSender(leaderId);
        RemoveServerRequest removeServerRequest = RemoveServerRequest.newBuilder().setOldServer(oldServer).build();
        return sender.removeServer(groupName, removeServerRequest);
    }
}
