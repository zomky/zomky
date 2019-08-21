package io.github.zomky.raft;

import io.github.zomky.IntegrationTest;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@IntegrationTest
class RaftProtocolTest {
/*
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftProtocolTest.class);

    private static final boolean PRE_VOTE = true;
    private static final boolean LEADER_STICKINESS = true;

    @Mock
    ElectionTimeout electionTimeout1, electionTimeout2, electionTimeout3;

    Node node1, node2, node3, raftServer4;

    @BeforeEach
    public void setUp() {
        BlockHound.builder()
                .allowBlockingCallsInside("java.io.FileInputStream", "readBytes")
                .install();

        node1 = NodeFactory.receive()
                .port(7000)
                .cluster(DEFAULT_THREE_NODE_CLUSTER)
                .start()
                .block();

        node2 = NodeFactory.receive()
                .port(7001)
                .cluster(DEFAULT_THREE_NODE_CLUSTER)
                .start()
                .block();

        node3 = NodeFactory.receive()
                .port(7002)
                .cluster(DEFAULT_THREE_NODE_CLUSTER)
                .start()
                .block();

        RSocket rSocket = NodeFactory.connect()
                .port(7000)
                .start()
                .block();

    }

    @Test
    void testElection() {
        given(electionTimeout1.nextRandom()).willReturn(Duration.ofMillis(300));
        given(electionTimeout2.nextRandom()).willReturn(Duration.ofSeconds(10));
//        given(electionTimeout3.nextRandom()).willReturn(Duration.ofMillis(300));

        RaftConfiguration.Builder templateRaftConfiguration = RaftConfiguration.builder()
            .preVote(PRE_VOTE)
            .leaderStickiness(LEADER_STICKINESS)
            .stateMachineEntryConverter(new KVStateMachineEntryConverter());

        RaftGroup raftServer1Group1 = RaftGroup.builder()
            .groupName("group1")
            .inMemoryRaftStorage()
            .raftConfiguration(templateRaftConfiguration, raftConfiguration -> raftConfiguration
                .stateMachine(new KVStateMachine1(7000))
                .electionTimeout(electionTimeout1)
            )
            .node(node1)
            .configuration(new Configuration(7000,7001))
            .build();

        node1.getRaftProtocol().addGroup(raftServer1Group1);

        RaftGroup raftServer2Group1 = RaftGroup.builder()
                .groupName("group1")
                .inMemoryRaftStorage()
                .raftConfiguration(templateRaftConfiguration, raftConfiguration -> raftConfiguration
                        .stateMachine(new KVStateMachine1(7001))
                        .electionTimeout(electionTimeout2)
                )
                .node(node2)
                .configuration(new Configuration(7000,7001))
                .build();

        node2.getRaftProtocol().addGroup(raftServer2Group1);


        RaftGroup raftServer3Group2 = RaftGroup.builder()
                .groupName("group2")
                .inMemoryRaftStorage()
                .raftConfiguration(templateRaftConfiguration, raftConfiguration -> raftConfiguration
                        .stateMachine(new KVStateMachine1(7002))
                        .electionTimeout(electionTimeout3)
                )
                .node(node3)
                .configuration(new Configuration(7002))
                .build();

        node3.getRaftProtocol().addGroup(raftServer3Group2);

        await().atMost(5, TimeUnit.SECONDS).until(() -> raftServer1Group1.getCurrentLeaderId() == 7000);
        await().atMost(1, TimeUnit.SECONDS).until(() -> raftServer2Group1.getCurrentLeaderId() == 7000);
        await().atMost(1, TimeUnit.SECONDS).until(() -> raftServer3Group2.getCurrentLeaderId() == 7002);

        assertThat(raftServer1Group1.isLeader()).isTrue();
        assertThat(raftServer2Group1.isFollower()).isTrue();
        assertThat(raftServer3Group2.isLeader()).isTrue();
    }

    @Test
    void testLogReplication() {
        testElection();

        KVStoreClient kvStoreClient = new KVStoreClient(7000);

        int nbEntries = 10;

        kvStoreClient.put("group1", Flux.range(1, nbEntries).delayElements(Duration.ofMillis(200)).map(i -> new KeyValue("key" + i, "val" + i)))
                .doOnSubscribe(subscription -> LOGGER.info("KVStoreClient started"))
                .doOnNext(s -> LOGGER.info("KVStoreClient received {}", s))
                .doOnComplete(() -> LOGGER.info("KVStoreClient finished"))
                .blockLast();
    }

    @AfterEach
    void tearDown() {
        node1.dispose();
        node2.dispose();
        node3.dispose();
        if (raftServer4 != null) {
            raftServer4.dispose();
        }
    }
*/
}