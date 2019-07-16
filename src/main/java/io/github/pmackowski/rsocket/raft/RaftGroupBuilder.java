package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.storage.RaftStorage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RaftGroupBuilder {

    private String name;
    private StateMachine stateMachine;
    private StateMachineEntryConverter stateMachineEntryConverter;
    private ElectionTimeout electionTimeout;
    private RaftStorage raftStorage;
    private List<Integer> nodes = new ArrayList<>();

    public RaftGroupBuilder name(String name) {
        this.name = name;
        return this;
    }

    public RaftGroupBuilder nodes(Integer ... nodes) {
        this.nodes.addAll(Arrays.asList(nodes));
        return this;
    }

    public RaftGroupBuilder stateMachine(StateMachine stateMachine) {
        this.stateMachine = stateMachine;
        return this;
    }

    public RaftGroupBuilder stateMachineEntryConverter(StateMachineEntryConverter stateMachineEntryConverter) {
        this.stateMachineEntryConverter = stateMachineEntryConverter;
        return this;
    }

    public RaftGroupBuilder electionTimeout(ElectionTimeout electionTimeout) {
        this.electionTimeout = electionTimeout;
        return this;
    }

    public RaftGroupBuilder storage(RaftStorage raftStorage) {
        this.raftStorage = raftStorage;
        return this;
    }

    public RaftGroup build() {
//        return new RaftGroup(raftStorage, name, nodes);
        return null;
    }
}
