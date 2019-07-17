package io.github.pmackowski.rsocket.raft.raft;

import java.nio.ByteBuffer;

public class RaftConfiguration {

    private static final boolean DEFAULT_PRE_VOTE = true;
    private static final boolean DEFAULT_LEADER_STICKINESS = true;
    public static final int ELECTION_TIMEOUT_MIN_IN_MILLIS = 150;

    private boolean preVote;
    private boolean leaderStickiness;
    private ElectionTimeout electionTimeout;
    private StateMachine<ByteBuffer> stateMachine;
    private StateMachineEntryConverter stateMachineEntryConverter;

    public boolean isPreVote() {
        return preVote;
    }

    public boolean isLeaderStickiness() {
        return leaderStickiness;
    }

    public ElectionTimeout getElectionTimeout() {
        return electionTimeout;
    }

    public StateMachine<ByteBuffer> getStateMachine() {
        return stateMachine;
    }

    public StateMachineEntryConverter getStateMachineEntryConverter() {
        return stateMachineEntryConverter;
    }

    private RaftConfiguration() {}

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(RaftConfiguration other) {
        return new Builder(other);
    }

    public static class Builder {

        private boolean preVote = DEFAULT_PRE_VOTE;
        private boolean leaderStickiness = DEFAULT_LEADER_STICKINESS;
        private ElectionTimeout electionTimeout = ElectionTimeout.random(ELECTION_TIMEOUT_MIN_IN_MILLIS);
        private StateMachine<ByteBuffer> stateMachine;
        private StateMachineEntryConverter stateMachineEntryConverter;

        private Builder() {
        }

        private Builder(RaftConfiguration other) {
            this.preVote = other.isPreVote();
            this.leaderStickiness = other.isLeaderStickiness();
            this.electionTimeout = other.getElectionTimeout();
            this.stateMachine = other.getStateMachine();
            this.stateMachineEntryConverter = other.getStateMachineEntryConverter();
        }

        public Builder preVote(boolean preVote) {
            this.preVote = preVote;
            return this;
        }

        public Builder leaderStickiness(boolean leaderStickiness) {
            this.leaderStickiness = leaderStickiness;
            return this;
        }

        public Builder electionTimeout(ElectionTimeout electionTimeout) {
            this.electionTimeout = electionTimeout;
            return this;
        }

        public Builder stateMachineName(String stateMachineName) {
            // TODO use reflections to find stateMachine and stateMachineEntryConverter
            return this;
        }

        public Builder stateMachine(StateMachine<ByteBuffer> stateMachine) {
            this.stateMachine = stateMachine;
            return this;
        }

        public Builder stateMachineEntryConverter(StateMachineEntryConverter stateMachineEntryConverter) {
            this.stateMachineEntryConverter = stateMachineEntryConverter;
            return this;
        }

        public RaftConfiguration build() {
            RaftConfiguration raftConfiguration = new RaftConfiguration();
            raftConfiguration.preVote = preVote;
            raftConfiguration.leaderStickiness = leaderStickiness;
            raftConfiguration.electionTimeout = electionTimeout;
            raftConfiguration.stateMachine = stateMachine;
            raftConfiguration.stateMachineEntryConverter = stateMachineEntryConverter;
            return raftConfiguration;
        }
    }
}
