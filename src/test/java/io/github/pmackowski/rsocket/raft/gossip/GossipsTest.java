package io.github.pmackowski.rsocket.raft.gossip;

import io.github.pmackowski.rsocket.raft.gossip.protobuf.Ack;
import io.github.pmackowski.rsocket.raft.gossip.protobuf.Gossip;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.github.pmackowski.rsocket.raft.gossip.protobuf.Gossip.Suspicion.*;
import static org.assertj.core.api.Assertions.assertThat;

class GossipsTest {

    static final int INITIATOR_NODE_ID = 7000;
    static final int DESTINATION_NODE_ID = 7001;

    @Test
    void probeCompletedInitializedWithEmptyGossips() {
        // given
        Gossips gossips = Gossips.builder().nodeId(INITIATOR_NODE_ID).build();

        ProbeResult probeResult = ProbeResult.builder()
                .destinationNodeId(DESTINATION_NODE_ID)
                .subgroupSize(0)
                .probeResult(new ProbeOperatorResult<>(false,
                    Ack.newBuilder()
                        .addGossips(Gossip.newBuilder().setNodeId(7004).setSuspicion(SUSPECT).setIncarnation(1).build())
                        .addGossips(Gossip.newBuilder().setNodeId(7005).setSuspicion(ALIVE).setIncarnation(0).build())
                        .build()
                    )
                )
                .build();

        // when
        gossips.probeCompleted(probeResult);

        // then
        // produced by probeCompleted
        assertThat(gossips.getGossip(DESTINATION_NODE_ID)).hasValue(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(ALIVE).setIncarnation(0).build());
        // copied from Ack
        assertThat(gossips.getGossip(7004)).hasValue(Gossip.newBuilder().setNodeId(7004).setSuspicion(SUSPECT).setIncarnation(1).build());
        assertThat(gossips.getGossip(7005)).hasValue(Gossip.newBuilder().setNodeId(7005).setSuspicion(ALIVE).setIncarnation(0).build());
        assertThat(gossips.count()).isEqualTo(3);
    }

    @Test
    void probeCompletedInitializedWithGossips() {
        // given
        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .addGossip(Gossip.newBuilder().setNodeId(7004).setSuspicion(SUSPECT).setIncarnation(1).build())
                .addGossip(Gossip.newBuilder().setNodeId(7005).setSuspicion(ALIVE).setIncarnation(0).build())
                .build();

        ProbeResult probeResult = ProbeResult.builder()
                .destinationNodeId(DESTINATION_NODE_ID)
                .subgroupSize(0)
                .probeResult(new ProbeOperatorResult<>(false,
                                Ack.newBuilder()
                                        .addGossips(Gossip.newBuilder().setNodeId(7004).setSuspicion(ALIVE).setIncarnation(0).build())
                                        .addGossips(Gossip.newBuilder().setNodeId(7006).setSuspicion(ALIVE).setIncarnation(0).build())
                                        .build()
                        )
                )
                .build();

        // when
        gossips.probeCompleted(probeResult);

        // then
        // produced by probeCompleted
        assertThat(gossips.getGossip(DESTINATION_NODE_ID)).hasValue(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(ALIVE).setIncarnation(0).build());
        // merged
        assertThat(gossips.getGossip(7004)).hasValue(Gossip.newBuilder().setNodeId(7004).setSuspicion(SUSPECT).setIncarnation(1).build());
        assertThat(gossips.getGossip(7005)).hasValue(Gossip.newBuilder().setNodeId(7005).setSuspicion(ALIVE).setIncarnation(0).build());
        assertThat(gossips.getGossip(7006)).hasValue(Gossip.newBuilder().setNodeId(7006).setSuspicion(ALIVE).setIncarnation(0).build());
        assertThat(gossips.count()).isEqualTo(4);
    }

    @Test
    void probeCompletedNoAcks() {
        // given
        Gossips gossips = Gossips.builder().nodeId(INITIATOR_NODE_ID).build();

        ProbeResult probeResult = ProbeResult.builder()
                .destinationNodeId(DESTINATION_NODE_ID)
                .subgroupSize(1)
                .probeResult(new ProbeOperatorResult<>(true))
                .build();

        // when
        gossips.probeCompleted(probeResult);

        // then
        assertThat(gossips.getGossip(DESTINATION_NODE_ID)).hasValue(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(SUSPECT).setIncarnation(0).build());
        assertThat(gossips.count()).isEqualTo(1);
    }

    @Test
    void probeCompletedNoAcksThenDoNotIncreaseDisseminationCount() {
        // given
        Gossip hotGossip = Gossip.newBuilder().setIncarnation(0).setNodeId(7004).setSuspicion(SUSPECT).build();

        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .addGossip(hotGossip)
                .build();

        ProbeResult probeResult = ProbeResult.builder()
                .destinationNodeId(DESTINATION_NODE_ID)
                .subgroupSize(1)
                .probeResult(new ProbeOperatorResult<>(true))
                .hotGossips(Collections.singletonList(hotGossip))
                .build();

        // when
        gossips.probeCompleted(probeResult);

        // then
        assertThat(gossips.getGossip(DESTINATION_NODE_ID)).hasValue(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(SUSPECT).setIncarnation(0).build());
        assertThat(gossips.count()).isEqualTo(2);
        assertThat(gossips.getDisseminatedCount(7004)).isEqualTo(0); // not increased dissemination count
    }

    @Test
    void probeCompletedHasAcksThenIncreaseDisseminationCount() {
        // given
        Gossip hotGossip = Gossip.newBuilder().setIncarnation(0).setNodeId(7004).setSuspicion(SUSPECT).build();

        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .addGossip(hotGossip)
                .build();

        ProbeResult probeResult = ProbeResult.builder()
                .destinationNodeId(DESTINATION_NODE_ID)
                .subgroupSize(1)
                .probeResult(new ProbeOperatorResult<>(true, Ack.newBuilder().build()))
                .hotGossips(Collections.singletonList(hotGossip))
                .build();

        // when
        gossips.probeCompleted(probeResult);

        // then
        assertThat(gossips.getGossip(DESTINATION_NODE_ID)).hasValue(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(ALIVE).setIncarnation(0).build());
        assertThat(gossips.count()).isEqualTo(2);
        assertThat(gossips.getDisseminatedCount(7004)).isEqualTo(1); // increased dissemination count
    }

    @Test
    void probeCompletedMultipleAcks() {
        // given
        Gossips gossips = Gossips.builder().nodeId(INITIATOR_NODE_ID).build();

        ProbeResult probeResult = ProbeResult.builder()
                .destinationNodeId(DESTINATION_NODE_ID)
                .subgroupSize(1)
                .probeResult(new ProbeOperatorResult<>(true,
                                Ack.newBuilder()
                                        .setNodeId(7001)
                                        .addGossips(Gossip.newBuilder().setNodeId(7004).setSuspicion(ALIVE).setIncarnation(1).build())
                                        .addGossips(Gossip.newBuilder().setNodeId(7005).setSuspicion(ALIVE).setIncarnation(0).build())
                                        .build(),
                                Ack.newBuilder()
                                        .setNodeId(7002)
                                        .addGossips(Gossip.newBuilder().setNodeId(7004).setSuspicion(ALIVE).setIncarnation(0).build())
                                        .addGossips(Gossip.newBuilder().setNodeId(7006).setSuspicion(ALIVE).setIncarnation(0).build())
                                        .build()

                        )
                )
                .build();

        // when
        gossips.probeCompleted(probeResult);

        // then
        // produced by probeCompleted
        assertThat(gossips.getGossip(DESTINATION_NODE_ID)).hasValue(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(ALIVE).setIncarnation(0).build());
        // merged
        assertThat(gossips.getGossip(7004)).hasValue(Gossip.newBuilder().setNodeId(7004).setSuspicion(ALIVE).setIncarnation(1).build());
        assertThat(gossips.getGossip(7005)).hasValue(Gossip.newBuilder().setNodeId(7005).setSuspicion(ALIVE).setIncarnation(0).build());
        assertThat(gossips.getGossip(7006)).hasValue(Gossip.newBuilder().setNodeId(7006).setSuspicion(ALIVE).setIncarnation(0).build());
        assertThat(gossips.count()).isEqualTo(4);
    }

    @Test
    void probeCompletedOnlyNacks() { // nack can be returned by indirect ping
        // given
        Gossips gossips = Gossips.builder().nodeId(INITIATOR_NODE_ID).build();

        ProbeResult probeResult = ProbeResult.builder()
                .destinationNodeId(DESTINATION_NODE_ID)
                .subgroupSize(2)
                .probeResult(new ProbeOperatorResult<>(true,
                                Ack.newBuilder()
                                        .setNodeId(7001)
                                        .setNack(true)
                                        .addGossips(Gossip.newBuilder().setNodeId(7004).setSuspicion(ALIVE).setIncarnation(1).build())
                                        .addGossips(Gossip.newBuilder().setNodeId(7005).setSuspicion(ALIVE).setIncarnation(0).build())
                                        .build(),
                                Ack.newBuilder()
                                        .setNodeId(7002)
                                        .setNack(true)
                                        .addGossips(Gossip.newBuilder().setNodeId(7004).setSuspicion(ALIVE).setIncarnation(0).build())
                                        .addGossips(Gossip.newBuilder().setNodeId(7006).setSuspicion(ALIVE).setIncarnation(0).build())
                                        .build()
                        )
                )
                .build();

        // when
        gossips.probeCompleted(probeResult);

        // then
        // produced by probeCompleted
        assertThat(gossips.getGossip(DESTINATION_NODE_ID)).hasValue(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(SUSPECT).setIncarnation(0).build());
        // merged
        assertThat(gossips.getGossip(7004)).hasValue(Gossip.newBuilder().setNodeId(7004).setSuspicion(ALIVE).setIncarnation(1).build());
        assertThat(gossips.getGossip(7005)).hasValue(Gossip.newBuilder().setNodeId(7005).setSuspicion(ALIVE).setIncarnation(0).build());
        assertThat(gossips.getGossip(7006)).hasValue(Gossip.newBuilder().setNodeId(7006).setSuspicion(ALIVE).setIncarnation(0).build());
        assertThat(gossips.count()).isEqualTo(4);
    }

    @Test
    void probeCompletedAckWinsWithNack() { // nack can be returned by indirect ping
        // given
        Gossips gossips = Gossips.builder().nodeId(INITIATOR_NODE_ID).build();

        ProbeResult probeResult = ProbeResult.builder()
                .destinationNodeId(DESTINATION_NODE_ID)
                .subgroupSize(2)
                .probeResult(new ProbeOperatorResult<>(true,
                                Ack.newBuilder()
                                        .setNodeId(7001)
                                        .addGossips(Gossip.newBuilder().setNodeId(7004).setSuspicion(ALIVE).setIncarnation(1).build())
                                        .addGossips(Gossip.newBuilder().setNodeId(7005).setSuspicion(ALIVE).setIncarnation(0).build())
                                        .build(),
                                Ack.newBuilder()
                                        .setNodeId(7002)
                                        .setNack(true)
                                        .addGossips(Gossip.newBuilder().setNodeId(7004).setSuspicion(ALIVE).setIncarnation(0).build())
                                        .addGossips(Gossip.newBuilder().setNodeId(7006).setSuspicion(ALIVE).setIncarnation(0).build())
                                        .build()

                        )
                )
                .build();

        // when
        gossips.probeCompleted(probeResult);

        // then
        // produced by probeCompleted
        assertThat(gossips.getGossip(DESTINATION_NODE_ID)).hasValue(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(ALIVE).setIncarnation(0).build());
        // merged
        assertThat(gossips.getGossip(7004)).hasValue(Gossip.newBuilder().setNodeId(7004).setSuspicion(ALIVE).setIncarnation(1).build());
        assertThat(gossips.getGossip(7005)).hasValue(Gossip.newBuilder().setNodeId(7005).setSuspicion(ALIVE).setIncarnation(0).build());
        assertThat(gossips.getGossip(7006)).hasValue(Gossip.newBuilder().setNodeId(7006).setSuspicion(ALIVE).setIncarnation(0).build());
        assertThat(gossips.count()).isEqualTo(4);
    }

    @Test
    void probeCompletedNodeIsSuspected() {
        // given
        Gossips gossips = Gossips.builder().nodeId(INITIATOR_NODE_ID).build();

        ProbeResult probeResult = ProbeResult.builder()
                .destinationNodeId(DESTINATION_NODE_ID)
                .subgroupSize(0)
                .probeResult(new ProbeOperatorResult<>(false,
                                Ack.newBuilder()
                                        .addGossips(Gossip.newBuilder().setNodeId(INITIATOR_NODE_ID).setSuspicion(SUSPECT).setIncarnation(0).build())
                                        .build()
                        )
                )
                .build();

        // when
        gossips.probeCompleted(probeResult);

        // then
        // produced by probeCompleted
        assertThat(gossips.getGossip(DESTINATION_NODE_ID)).hasValue(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(ALIVE).setIncarnation(0).build());
        // merged
        assertThat(gossips.getGossip(INITIATOR_NODE_ID)).hasValue(Gossip.newBuilder().setNodeId(INITIATOR_NODE_ID).setSuspicion(ALIVE).setIncarnation(1).build());
        assertThat(gossips.count()).isEqualTo(2);
        assertThat(gossips.localHealthMultiplier()).isEqualTo(1);
    }

    @Test
    void probeCompletedProbedNodeIsSuspected() {
        // given
        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .addGossip(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(SUSPECT).setIncarnation(3).build())
                .build();

        ProbeResult probeResult = ProbeResult.builder()
                .destinationNodeId(DESTINATION_NODE_ID)
                .subgroupSize(0)
                .probeResult(new ProbeOperatorResult<>(false,
                                Ack.newBuilder()
                                        // we expect suspected node to refute suspicion by sending alive message with increased incarnation number
                                        .addGossips(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(ALIVE).setIncarnation(3+1).build())
                                        .build()
                        )
                )
                .build();

        // when
        gossips.probeCompleted(probeResult);

        // then
        // produced by probeCompleted
        assertThat(gossips.getGossip(DESTINATION_NODE_ID)).hasValue(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(ALIVE).setIncarnation(4).build());
        assertThat(gossips.count()).isEqualTo(1);
        assertThat(gossips.localHealthMultiplier()).isEqualTo(0);
    }

    @Test
    void probeCompletedProducedIncarnationNeverDecreases() {
        // given
        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .addGossip(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(ALIVE).setIncarnation(3).build())
                .build();

        ProbeResult probeResult = ProbeResult.builder()
                .destinationNodeId(DESTINATION_NODE_ID)
                .subgroupSize(2)
                .probeResult(new ProbeOperatorResult<>(true))
                .build();

        // when
        gossips.probeCompleted(probeResult);

        // then
        // produced by probeCompleted
        assertThat(gossips.getGossip(DESTINATION_NODE_ID)).hasValue(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(SUSPECT).setIncarnation(3).build());
        assertThat(gossips.count()).isEqualTo(1);
    }

    @Test
    void addAliveGossipAboutItself() {
        // given
        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .addAliveGossipAboutItself()
                .build();

        // then
        assertThat(gossips.getGossip(INITIATOR_NODE_ID))
                .hasValue(Gossip.newBuilder().setNodeId(INITIATOR_NODE_ID).setSuspicion(ALIVE).setIncarnation(0).build());
        assertThat(gossips.count()).isEqualTo(1);
    }

    @Test
    void addDeadGossipAboutItself() { // incarnation number does not matter
        // given
        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .addGossip(Gossip.newBuilder().setNodeId(INITIATOR_NODE_ID).setSuspicion(ALIVE).setIncarnation(3).build())
                .build();

        // when
        gossips.addGossip(Gossip.newBuilder().setNodeId(INITIATOR_NODE_ID).setSuspicion(DEAD).setIncarnation(0).build());

        // then
        assertThat(gossips.getGossip(INITIATOR_NODE_ID))
                .hasValue(Gossip.newBuilder().setNodeId(INITIATOR_NODE_ID).setSuspicion(DEAD).setIncarnation(0).build());
        assertThat(gossips.count()).isEqualTo(1);
    }

    @Test
    void addSuspectGossipAboutItselfWithSameIncarnationNumber() {
        // given
        Gossips gossips = Gossips.builder().nodeId(INITIATOR_NODE_ID).build();

        // when
        gossips.addGossip(Gossip.newBuilder().setNodeId(INITIATOR_NODE_ID).setSuspicion(SUSPECT).setIncarnation(0).build());

        // then
        assertThat(gossips.getGossip(INITIATOR_NODE_ID))
                .hasValue(Gossip.newBuilder().setNodeId(INITIATOR_NODE_ID).setSuspicion(ALIVE).setIncarnation(1).build());
        assertThat(gossips.incarnation()).isEqualTo(1);
        assertThat(gossips.count()).isEqualTo(1);
        assertThat(gossips.localHealthMultiplier()).isEqualTo(1); // suspicion about itself increases LHM
    }

    @Test
    void addSuspectGossipAboutItselfWithDifferentIncarnationNumber() {
        // given
        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .incarnation(1)
                .build();

        // when
        gossips.addGossip(Gossip.newBuilder().setNodeId(INITIATOR_NODE_ID).setSuspicion(SUSPECT).setIncarnation(0).build());

        // then
        assertThat(gossips.incarnation()).isEqualTo(1);
        assertThat(gossips.count()).isEqualTo(0);
        assertThat(gossips.localHealthMultiplier()).isEqualTo(0);
    }

    @Test
    void addAliveGossipAboutItselfIsIgnored() {
        // given
        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .incarnation(0)
                .build();

        // when
        gossips.addGossip(Gossip.newBuilder().setNodeId(INITIATOR_NODE_ID).setSuspicion(ALIVE).setIncarnation(0).build());

        // then
        assertThat(gossips.incarnation()).isEqualTo(0);
        assertThat(gossips.count()).isEqualTo(0);
    }

    @Test
    void addAliveGossipAboutItselfWithGreaterIncarnationNumberIsAlsoIgnored() {
        // impossible situation as only node itself can increase its incarnation number,
        // solution - silently ignore gossip

        // given
        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .addGossip(Gossip.newBuilder().setNodeId(INITIATOR_NODE_ID).setSuspicion(ALIVE).setIncarnation(1).build())
                .incarnation(1)
                .build();

        // when
        gossips.addGossip(Gossip.newBuilder().setNodeId(INITIATOR_NODE_ID).setSuspicion(ALIVE).setIncarnation(2).build());

        // then
        assertThat(gossips.incarnation()).isEqualTo(1);
        assertThat(gossips.getGossip(INITIATOR_NODE_ID))
                .hasValue(Gossip.newBuilder().setNodeId(INITIATOR_NODE_ID).setSuspicion(ALIVE).setIncarnation(1).build());

        assertThat(gossips.count()).isEqualTo(1);
    }

    @Test
    void addDeadGossip() { // incarnation number does not matter
        // given
        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .addGossip(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(ALIVE).setIncarnation(3).build())
                .build();

        // when
        gossips.addGossip(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(DEAD).setIncarnation(0).build());

        // then
        assertThat(gossips.getGossip(DESTINATION_NODE_ID))
                .hasValue(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(DEAD).setIncarnation(0).build());
        assertThat(gossips.count()).isEqualTo(1);
    }

    @Test
    void addAliveGossipWhichOverrideOtherAlive() {
        // given
        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .addGossip(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(ALIVE).setIncarnation(3).build(), 10)
                .build();

        // when
        gossips.addGossip(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(ALIVE).setIncarnation(4).build());

        // then
        assertThat(gossips.getGossip(DESTINATION_NODE_ID))
                .hasValue(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(ALIVE).setIncarnation(4).build());
        assertThat(gossips.getDisseminatedCount(DESTINATION_NODE_ID)).isEqualTo(0);

        assertThat(gossips.count()).isEqualTo(1);
    }

    @Test
    void addAliveGossipWhichDoesNotOverrideOtherAlive() {
        // given
        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .addGossip(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(ALIVE).setIncarnation(3).build(), 10)
                .build();

        // when
        gossips.addGossip(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(ALIVE).setIncarnation(3).build());

        // then
        assertThat(gossips.getGossip(DESTINATION_NODE_ID))
                .hasValue(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(ALIVE).setIncarnation(3).build());
        assertThat(gossips.getDisseminatedCount(DESTINATION_NODE_ID)).isEqualTo(10);

        assertThat(gossips.count()).isEqualTo(1);
    }

    @Test
    void addAliveGossipWhichOverrideSuspectGossip() {
        // given
        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .addGossip(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(SUSPECT).setIncarnation(3).build(), 10)
                .build();

        // when
        gossips.addGossip(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(ALIVE).setIncarnation(4).build());

        // then
        assertThat(gossips.getGossip(DESTINATION_NODE_ID))
                .hasValue(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(ALIVE).setIncarnation(4).build());
        assertThat(gossips.getDisseminatedCount(DESTINATION_NODE_ID)).isEqualTo(0);

        assertThat(gossips.count()).isEqualTo(1);
    }

    @Test
    void addAliveGossipWhichDoesNotOverrideSuspectGossip() {
        // given
        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .addGossip(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(SUSPECT).setIncarnation(3).build(), 10)
                .build();

        // when
        gossips.addGossip(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(ALIVE).setIncarnation(3).build());

        // then
        assertThat(gossips.getGossip(DESTINATION_NODE_ID))
                .hasValue(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(SUSPECT).setIncarnation(3).build());
        assertThat(gossips.getDisseminatedCount(DESTINATION_NODE_ID)).isEqualTo(10);

        assertThat(gossips.count()).isEqualTo(1);
    }

    @Test
    void addSuspectGossipWhichOverrideOtherSuspectGossip() {
        // given
        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .addGossip(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(SUSPECT).setIncarnation(3).build(), 10)
                .build();

        // when
        gossips.addGossip(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(SUSPECT).setIncarnation(4).build());

        // then
        assertThat(gossips.getGossip(DESTINATION_NODE_ID))
                .hasValue(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(SUSPECT).setIncarnation(4).build());
        assertThat(gossips.getDisseminatedCount(DESTINATION_NODE_ID)).isEqualTo(0);

        assertThat(gossips.count()).isEqualTo(1);
    }

    @Test
    void addSuspectGossipWhichDoesNotOverrideOtherSuspectGossip() {
        // given
        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .addGossip(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(SUSPECT).setIncarnation(3).build(), 10)
                .build();

        // when
        gossips.addGossip(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(SUSPECT).setIncarnation(3).build());

        // then
        assertThat(gossips.getGossip(DESTINATION_NODE_ID))
                .hasValue(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(SUSPECT).setIncarnation(3).build());
        assertThat(gossips.getDisseminatedCount(DESTINATION_NODE_ID)).isEqualTo(10);

        assertThat(gossips.count()).isEqualTo(1);
    }

    @Test
    void addSuspectGossipWhichOverrideAliveGossip() {
        // given
        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .addGossip(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(ALIVE).setIncarnation(3).build(), 10)
                .build();

        // when
        gossips.addGossip(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(SUSPECT).setIncarnation(3).build());

        // then
        assertThat(gossips.getGossip(DESTINATION_NODE_ID))
                .hasValue(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(SUSPECT).setIncarnation(3).build());
        assertThat(gossips.getDisseminatedCount(DESTINATION_NODE_ID)).isEqualTo(0);

        assertThat(gossips.count()).isEqualTo(1);
    }

    @Test
    void addSuspectGossipWhichDoesNotOverrideAliveGossip() {
        // given
        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .addGossip(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(ALIVE).setIncarnation(4).build(), 10)
                .build();

        // when
        gossips.addGossip(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(SUSPECT).setIncarnation(3).build());

        // then
        assertThat(gossips.getGossip(DESTINATION_NODE_ID))
                .hasValue(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(ALIVE).setIncarnation(4).build());
        assertThat(gossips.getDisseminatedCount(DESTINATION_NODE_ID)).isEqualTo(10);

        assertThat(gossips.count()).isEqualTo(1);
    }

    @Test
    void addSuspectGossipNeverOverrideDeadGossip() {
        // given
        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .addGossip(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(DEAD).setIncarnation(0).build(), 10)
                .build();

        // when
        gossips.addGossip(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(SUSPECT).setIncarnation(1).build());

        // then
        assertThat(gossips.getGossip(DESTINATION_NODE_ID))
                .hasValue(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(DEAD).setIncarnation(0).build());
        assertThat(gossips.getDisseminatedCount(DESTINATION_NODE_ID)).isEqualTo(10);

        assertThat(gossips.count()).isEqualTo(1);
    }

    @Test
    void addAliveGossipNeverOverrideDeadGossip() {
        // given
        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .addGossip(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(DEAD).setIncarnation(0).build(), 10)
                .build();

        // when
        gossips.addGossip(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(ALIVE).setIncarnation(1).build());

        // then
        assertThat(gossips.getGossip(DESTINATION_NODE_ID))
                .hasValue(Gossip.newBuilder().setNodeId(DESTINATION_NODE_ID).setSuspicion(DEAD).setIncarnation(0).build());
        assertThat(gossips.getDisseminatedCount(DESTINATION_NODE_ID)).isEqualTo(10);

        assertThat(gossips.count()).isEqualTo(1);
    }

    @Test
    void chooseHotGossipsNoGossips() {
        // given
        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .build();

        // when
        List<Gossip> actual = gossips.chooseHotGossips();

        // then
        assertThat(gossips.maxGossipDissemination()).isEqualTo(0);
        assertThat(actual).isEmpty();
    }

    @Test
    void chooseHotGossipsForOneGossipNotDisseminatedSoFar() {
        // given
        Gossip gossip = Gossip.newBuilder().setNodeId(INITIATOR_NODE_ID).setSuspicion(ALIVE).setIncarnation(0).build();
        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .addGossip(gossip, 0)
                .build();

        // when
        List<Gossip> actual = gossips.chooseHotGossips();

        // then
        assertThat(gossips.maxGossipDissemination()).isEqualTo(1);
        assertThat(actual).contains(gossip);
    }

    @Test
    void chooseHotGossipsForOneGossipAlreadyDisseminated() {
        // given
        Gossip gossip = Gossip.newBuilder().setNodeId(INITIATOR_NODE_ID).setSuspicion(ALIVE).setIncarnation(0).build();
        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .addGossip(gossip, 1)
                .build();

        // when
        List<Gossip> actual = gossips.chooseHotGossips();

        // then
        assertThat(gossips.maxGossipDissemination()).isEqualTo(1);
        assertThat(actual).isEmpty();
    }

    @Test
    void chooseHotGossips() {
        // given
        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .addGossip(Gossip.newBuilder().setNodeId(7001).setSuspicion(ALIVE).setIncarnation(0).build(), 4)
                .addGossip(Gossip.newBuilder().setNodeId(7002).setSuspicion(SUSPECT).setIncarnation(1).build(), 0)
                .addGossip(Gossip.newBuilder().setNodeId(7003).setSuspicion(SUSPECT).setIncarnation(1).build(), 1)
                .addGossip(Gossip.newBuilder().setNodeId(7004).setSuspicion(SUSPECT).setIncarnation(1).build(), 5)
                .addGossip(Gossip.newBuilder().setNodeId(7005).setSuspicion(SUSPECT).setIncarnation(1).build(), 5)
                .addGossip(Gossip.newBuilder().setNodeId(7006).setSuspicion(SUSPECT).setIncarnation(1).build(), 1)
                .addGossip(Gossip.newBuilder().setNodeId(7007).setSuspicion(DEAD).setIncarnation(1).build(), 4)
                .addGossip(Gossip.newBuilder().setNodeId(7008).setSuspicion(SUSPECT).setIncarnation(1).build(), 4)
                .addGossip(Gossip.newBuilder().setNodeId(7009).setSuspicion(ALIVE).setIncarnation(1).build(), 1)
                .addGossip(Gossip.newBuilder().setNodeId(7010).setSuspicion(SUSPECT).setIncarnation(1).build(), 5)
                .build();

        // when
        List<Gossip> actual = gossips.chooseHotGossips();

        // then
        assertThat(gossips.maxGossipDissemination()).isEqualTo(5);
        assertThat(actual).containsExactlyInAnyOrder(
                Gossip.newBuilder().setNodeId(7001).setSuspicion(ALIVE).setIncarnation(0).build(),
                Gossip.newBuilder().setNodeId(7002).setSuspicion(SUSPECT).setIncarnation(1).build(),
                Gossip.newBuilder().setNodeId(7003).setSuspicion(SUSPECT).setIncarnation(1).build(),
                Gossip.newBuilder().setNodeId(7006).setSuspicion(SUSPECT).setIncarnation(1).build(),
                Gossip.newBuilder().setNodeId(7007).setSuspicion(DEAD).setIncarnation(1).build(),
                Gossip.newBuilder().setNodeId(7008).setSuspicion(SUSPECT).setIncarnation(1).build(),
                Gossip.newBuilder().setNodeId(7009).setSuspicion(ALIVE).setIncarnation(1).build()
        );
    }

    @Test
    void chooseHotGossipsWithMultiplier() {
        // given
        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .gossipDisseminationMultiplier(1.6f)
                .addGossip(Gossip.newBuilder().setNodeId(7001).setSuspicion(ALIVE).setIncarnation(0).build(), 4)
                .addGossip(Gossip.newBuilder().setNodeId(7002).setSuspicion(SUSPECT).setIncarnation(1).build(), 0)
                .addGossip(Gossip.newBuilder().setNodeId(7003).setSuspicion(SUSPECT).setIncarnation(1).build(), 7)
                .addGossip(Gossip.newBuilder().setNodeId(7004).setSuspicion(SUSPECT).setIncarnation(1).build(), 8)
                .addGossip(Gossip.newBuilder().setNodeId(7005).setSuspicion(SUSPECT).setIncarnation(1).build(), 8)
                .addGossip(Gossip.newBuilder().setNodeId(7006).setSuspicion(SUSPECT).setIncarnation(1).build(), 1)
                .addGossip(Gossip.newBuilder().setNodeId(7007).setSuspicion(DEAD).setIncarnation(1).build(), 4)
                .addGossip(Gossip.newBuilder().setNodeId(7008).setSuspicion(SUSPECT).setIncarnation(1).build(), 7)
                .addGossip(Gossip.newBuilder().setNodeId(7009).setSuspicion(ALIVE).setIncarnation(1).build(), 1)
                .addGossip(Gossip.newBuilder().setNodeId(7010).setSuspicion(SUSPECT).setIncarnation(1).build(), 8)
                .build();

        // when
        List<Gossip> actual = gossips.chooseHotGossips();

        // then
        assertThat(gossips.maxGossipDissemination()).isEqualTo(8);
        assertThat(actual).containsExactlyInAnyOrder(
                Gossip.newBuilder().setNodeId(7001).setSuspicion(ALIVE).setIncarnation(0).build(),
                Gossip.newBuilder().setNodeId(7002).setSuspicion(SUSPECT).setIncarnation(1).build(),
                Gossip.newBuilder().setNodeId(7003).setSuspicion(SUSPECT).setIncarnation(1).build(),
                Gossip.newBuilder().setNodeId(7006).setSuspicion(SUSPECT).setIncarnation(1).build(),
                Gossip.newBuilder().setNodeId(7007).setSuspicion(DEAD).setIncarnation(1).build(),
                Gossip.newBuilder().setNodeId(7008).setSuspicion(SUSPECT).setIncarnation(1).build(),
                Gossip.newBuilder().setNodeId(7009).setSuspicion(ALIVE).setIncarnation(1).build()
        );
    }

    @Test
    void chooseHotGossipsExceptOnesThatShouldBeIgnored() {
        // given
        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .addGossip(Gossip.newBuilder().setNodeId(7001).setSuspicion(ALIVE).setIncarnation(0).build(), 2)
                .addGossip(Gossip.newBuilder().setNodeId(7002).setSuspicion(SUSPECT).setIncarnation(1).build(), 0)
                .addGossip(Gossip.newBuilder().setNodeId(7003).setSuspicion(SUSPECT).setIncarnation(1).build(), 1)
                .addGossip(Gossip.newBuilder().setNodeId(7004).setSuspicion(SUSPECT).setIncarnation(1).build(), 3)
                .addGossip(Gossip.newBuilder().setNodeId(7005).setSuspicion(SUSPECT).setIncarnation(1).build(), 5)
                .addGossip(Gossip.newBuilder().setNodeId(7006).setSuspicion(SUSPECT).setIncarnation(1).build(), 5)
                .addGossip(Gossip.newBuilder().setNodeId(7007).setSuspicion(DEAD).setIncarnation(1).build(), 5)
                .addGossip(Gossip.newBuilder().setNodeId(7008).setSuspicion(SUSPECT).setIncarnation(1).build(), 5)
                .addGossip(Gossip.newBuilder().setNodeId(7009).setSuspicion(ALIVE).setIncarnation(1).build(), 5)
                .addGossip(Gossip.newBuilder().setNodeId(7010).setSuspicion(SUSPECT).setIncarnation(1).build(), 5)
                .build();
        List<Gossip> ignoreGossips = Arrays.asList(
            Gossip.newBuilder().setNodeId(7003).setSuspicion(SUSPECT).setIncarnation(1).build(),
            Gossip.newBuilder().setNodeId(7004).setSuspicion(SUSPECT).setIncarnation(1).build()
        );

        // when
        List<Gossip> actual = gossips.chooseHotGossips(ignoreGossips);

        // then
        assertThat(gossips.maxGossipDissemination()).isEqualTo(5);
        assertThat(actual).containsExactlyInAnyOrder(
                Gossip.newBuilder().setNodeId(7001).setSuspicion(ALIVE).setIncarnation(0).build(),
                Gossip.newBuilder().setNodeId(7002).setSuspicion(SUSPECT).setIncarnation(1).build()
        );
    }

    @Test
    void chooseHotGossipsLimitedByMaxGossips() {
        // given
        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .maxGossips(2)
                .addGossip(Gossip.newBuilder().setNodeId(7001).setSuspicion(ALIVE).setIncarnation(0).build(), 1)
                .addGossip(Gossip.newBuilder().setNodeId(7002).setSuspicion(SUSPECT).setIncarnation(1).build(), 3)
                .addGossip(Gossip.newBuilder().setNodeId(7003).setSuspicion(SUSPECT).setIncarnation(1).build(), 7)
                .addGossip(Gossip.newBuilder().setNodeId(7004).setSuspicion(SUSPECT).setIncarnation(1).build(), 2)
                .addGossip(Gossip.newBuilder().setNodeId(7005).setSuspicion(SUSPECT).setIncarnation(1).build(), 0)
                .addGossip(Gossip.newBuilder().setNodeId(7006).setSuspicion(SUSPECT).setIncarnation(1).build(), 5)
                .addGossip(Gossip.newBuilder().setNodeId(7007).setSuspicion(DEAD).setIncarnation(1).build(), 4)
                .addGossip(Gossip.newBuilder().setNodeId(7008).setSuspicion(SUSPECT).setIncarnation(1).build(), 0)
                .addGossip(Gossip.newBuilder().setNodeId(7009).setSuspicion(ALIVE).setIncarnation(1).build(), 1)
                .addGossip(Gossip.newBuilder().setNodeId(7010).setSuspicion(SUSPECT).setIncarnation(1).build(), 2)
                .build();

        // when
        List<Gossip> actual = gossips.chooseHotGossips();

        // then
        assertThat(gossips.maxGossipDissemination()).isEqualTo(5);
        assertThat(actual).containsExactlyInAnyOrder(
                Gossip.newBuilder().setNodeId(7005).setSuspicion(SUSPECT).setIncarnation(1).build(),
                Gossip.newBuilder().setNodeId(7008).setSuspicion(SUSPECT).setIncarnation(1).build()
        );
    }

    @Test
    void chooseHotGossipsAll() {
        // given
        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .maxGossips(1)
                .gossipDisseminationMultiplier(0.5f)
                .addGossip(Gossip.newBuilder().setNodeId(7001).setSuspicion(ALIVE).setIncarnation(0).build(), 2)
                .addGossip(Gossip.newBuilder().setNodeId(7002).setSuspicion(SUSPECT).setIncarnation(1).build(), 0)
                .addGossip(Gossip.newBuilder().setNodeId(7003).setSuspicion(SUSPECT).setIncarnation(1).build(), 1)
                .addGossip(Gossip.newBuilder().setNodeId(7004).setSuspicion(SUSPECT).setIncarnation(1).build(), 3)
                .addGossip(Gossip.newBuilder().setNodeId(7005).setSuspicion(SUSPECT).setIncarnation(1).build(), 5)
                .addGossip(Gossip.newBuilder().setNodeId(7006).setSuspicion(SUSPECT).setIncarnation(1).build(), 5)
                .addGossip(Gossip.newBuilder().setNodeId(7007).setSuspicion(DEAD).setIncarnation(1).build(), 5)
                .addGossip(Gossip.newBuilder().setNodeId(7008).setSuspicion(SUSPECT).setIncarnation(1).build(), 5)
                .addGossip(Gossip.newBuilder().setNodeId(7009).setSuspicion(ALIVE).setIncarnation(1).build(), 5)
                .addGossip(Gossip.newBuilder().setNodeId(7010).setSuspicion(SUSPECT).setIncarnation(1).build(), 5)
                .build();
        List<Gossip> ignoreGossips = Arrays.asList(
                Gossip.newBuilder().setNodeId(7003).setSuspicion(SUSPECT).setIncarnation(1).build(),
                Gossip.newBuilder().setNodeId(7004).setSuspicion(SUSPECT).setIncarnation(1).build()
        );

        // when
        List<Gossip> actual = gossips.chooseHotGossips(ignoreGossips);

        // then
        assertThat(gossips.maxGossipDissemination()).isEqualTo(3);
        assertThat(actual).containsExactlyInAnyOrder(
                Gossip.newBuilder().setNodeId(7002).setSuspicion(SUSPECT).setIncarnation(1).build()
        );
    }

    @Test
    void makeGossipsLessHot() {
        // given
        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .addGossip(Gossip.newBuilder().setNodeId(7001).setSuspicion(ALIVE).setIncarnation(0).build(), 20)
                .addGossip(Gossip.newBuilder().setNodeId(7002).setSuspicion(SUSPECT).setIncarnation(1).build(), 10)
                .addGossip(Gossip.newBuilder().setNodeId(7003).setSuspicion(SUSPECT).setIncarnation(1).build(), 0)
                .addGossip(Gossip.newBuilder().setNodeId(7004).setSuspicion(SUSPECT).setIncarnation(1).build(), 22)
                .build();

        // when
        gossips.makeGossipsLessHot(Arrays.asList(
                Gossip.newBuilder().setNodeId(7001).setSuspicion(ALIVE).setIncarnation(0).build(),
                Gossip.newBuilder().setNodeId(7003).setSuspicion(SUSPECT).setIncarnation(1).build()
        ));

        // then
        assertThat(gossips.getDisseminatedCount(7001)).isEqualTo(21);
        assertThat(gossips.getDisseminatedCount(7002)).isEqualTo(10);
        assertThat(gossips.getDisseminatedCount(7003)).isEqualTo(1);
        assertThat(gossips.getDisseminatedCount(7004)).isEqualTo(22);

        assertThat(gossips.count()).isEqualTo(4);
    }

    @DisplayName("Gossip dissemination count")
    @ParameterizedTest(name = "Gossip should be disseminated exactly {2} times for {0} peers and multiplier {1}")
    @CsvSource({
            "0, 1f, 0",
            "1, 1f, 1",
            "2, 1f, 2",
            "3, 1f, 3",
            "4, 1f, 3",
            "5, 1f, 4",
            "6, 1f, 4",
            "7, 1f, 4",
            "8, 1f, 4",
            "16, 1f, 5",
            "32, 1f, 6",
            "64, 1f, 7",
            "128, 1f, 8",
            "256, 1f, 9",
            "512, 1f, 10",
            // with multiplier
            "0, 1.5f, 0",
            "1, 1.5f, 2",
            "2, 1.5f, 3",
            "3, 1.5f, 5",
            "4, 1.5f, 5",
            "5, 1.5f, 6",
            "6, 1.5f, 6",
            "7, 1.5f, 6",
            "8, 1.5f, 6",
            "16, 1.5f, 8",
            "32, 1.5f, 9",
            "64, 1.5f, 11",
            "128, 1.5f, 12",
            "256, 1.5f, 14",
            "512, 1.5f, 15"
    })
    void gossipDisseminationCount(int peers, float multiplier, int expectedGossips) {
        // given
        Gossips gossips = Gossips.builder()
                .gossipDisseminationMultiplier(multiplier)
                .build();
        // when
        int actual = gossips.maxGossipDissemination(peers);

        // then
        assertThat(actual).isEqualTo(expectedGossips);
    }

    @Test
    void updateLocalHealthMultiplier() {
        // given
        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .build();

        ProbeResult probeResult = ProbeResult.builder()
                .destinationNodeId(DESTINATION_NODE_ID)
                .subgroupSize(0)
                .probeResult(new ProbeOperatorResult<>(false,
                                Ack.newBuilder()
                                        .addGossips(Gossip.newBuilder().setNodeId(7004).setSuspicion(ALIVE).setIncarnation(0).build())
                                        .addGossips(Gossip.newBuilder().setNodeId(7006).setSuspicion(ALIVE).setIncarnation(0).build())
                                        .build()
                        )
                )
                .build();

        // when
        gossips.updateLocalHealthMultiplier(probeResult);

        // then
        assertThat(gossips.localHealthMultiplier()).isEqualTo(0);
    }

    @Test
    void updateLocalHealthMultiplierNoAcks() {
        // given
        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .build();

        ProbeResult probeResult = ProbeResult.builder()
                .destinationNodeId(DESTINATION_NODE_ID)
                .subgroupSize(2)
                .probeResult(new ProbeOperatorResult<>(true))
                .build();

        // when
        gossips.updateLocalHealthMultiplier(probeResult);

        // then
        assertThat(gossips.localHealthMultiplier()).isEqualTo(1);
    }


    @Test
    void updateLocalHealthMultiplierNoAcksAndMissedNack() {
        // given
        Gossips gossips = Gossips.builder()
                .nodeId(INITIATOR_NODE_ID)
                .build();

        ProbeResult probeResult = ProbeResult.builder()
                .destinationNodeId(DESTINATION_NODE_ID)
                .subgroupSize(2)
                .missedNack(true)
                .probeResult(new ProbeOperatorResult<>(true))
                .build();

        // when
        gossips.updateLocalHealthMultiplier(probeResult);

        // then
        assertThat(gossips.localHealthMultiplier()).isEqualTo(2);
    }

}