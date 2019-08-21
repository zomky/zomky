package io.github.zomky.client;

import io.github.zomky.gossip.protobuf.InitJoinRequest;
import io.github.zomky.gossip.protobuf.InitJoinResponse;
import io.github.zomky.gossip.protobuf.InitLeaveRequest;
import io.github.zomky.gossip.protobuf.InitLeaveResponse;
import io.github.zomky.gossip.transport.GossipTcpTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.net.InetAddress;

public class ClusterManagementClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterManagementClient.class);

    public Mono<InitJoinResponse> initJoin(Integer agentPort, InetAddress host, int port) {
        InitJoinRequest initJoinRequest = InitJoinRequest.newBuilder()
                .setRequesterPort(agentPort)
                .setHost(host.getHostAddress())
                .setPort(port)
                .build();

        return GossipTcpTransport.initJoin(initJoinRequest);
    }

    public Mono<InitLeaveResponse> initLeave(Integer agentPort) {
        InitLeaveRequest initLeaveRequest = InitLeaveRequest.newBuilder()
                .setRequesterPort(agentPort)
                .build();

        return GossipTcpTransport.initLeave(initLeaveRequest);
    }
}
