package io.github.pmackowski.rsocket.raft.client;

import io.github.pmackowski.rsocket.raft.client.protobuf.JoinRequest;
import io.github.pmackowski.rsocket.raft.client.protobuf.JoinResponse;
import io.github.pmackowski.rsocket.raft.client.protobuf.LeaveRequest;
import io.github.pmackowski.rsocket.raft.client.protobuf.LeaveResponse;
import io.github.pmackowski.rsocket.raft.transport.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.net.InetAddress;

public class ClusterManagementClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterManagementClient.class);

    public Mono<JoinResponse> join(Integer agentPort, InetAddress host, int port) {
        Sender sender = Sender.createSender(agentPort);
        JoinRequest joinRequest = JoinRequest.newBuilder()
                .setRequesterPort(agentPort)
                .setHost(host.getHostAddress())
                .setPort(port)
                .build();

        return sender.join(joinRequest);
    }

    public Mono<LeaveResponse> leave(Integer agentPort) {
        Sender sender = Sender.createSender(agentPort);
        LeaveRequest leaveRequest = LeaveRequest.newBuilder()
                .setRequesterPort(agentPort)
                .build();

        return sender.leave(leaveRequest);
    }
}
