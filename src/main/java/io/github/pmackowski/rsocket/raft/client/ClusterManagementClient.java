package io.github.pmackowski.rsocket.raft.client;

import io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest;
import io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinResponse;
import io.github.pmackowski.rsocket.raft.transport.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.net.InetAddress;

public class ClusterManagementClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterManagementClient.class);

    public Mono<InitJoinResponse> join(Integer agentPort, InetAddress host, int port) {
        Sender sender = Sender.createSender(agentPort);
        InitJoinRequest initJoinRequest = InitJoinRequest.newBuilder()
                .setRequesterPort(agentPort)
                .setHost(host.getHostAddress())
                .setPort(port)
                .build();

        return sender.initJoin(initJoinRequest);
    }
}
