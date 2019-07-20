package io.github.pmackowski.rsocket.raft.cli.runner;

import io.github.pmackowski.rsocket.raft.cli.ZomkyCommandRunner;
import io.github.pmackowski.rsocket.raft.cli.command.AgentCommand;
import io.github.pmackowski.rsocket.raft.storage.FileSystemRaftStorage;
import io.github.pmackowski.rsocket.raft.storage.InMemoryRaftStorage;
import io.github.pmackowski.rsocket.raft.storage.RaftStorage;
import io.github.pmackowski.rsocket.raft.storage.RaftStorageConfiguration;
import io.github.pmackowski.rsocket.raft.storage.log.SizeUnit;
import picocli.CommandLine.ParseResult;

public class AgentCommandRunner implements ZomkyCommandRunner {

    @Override
    public boolean support(ParseResult parseResult) {
        return support("agent", parseResult);
    }

    @Override
    public void execute(ParseResult parseResult) {
/*        MainCommand mainCommand = mainCommand(parseResult);
        AgentCommand agentCommand = command(parseResult, AgentCommand.class);

        Mono<Node> raftServerMono = new NodeFactory()
                .storage(raftStorage(agentCommand))
                .leaderStickiness(agentCommand.isLeaderStickiness())
                .preVote(agentCommand.isPreVote())
                .nodeId(mainCommand.getPort())
                .passive(agentCommand.isPassive())
                .electionTimeout(new ElectionTimeout())
                .initialConfiguration(new Configuration(mainCommand.getPort()))
                .stateMachine(stateMachine(mainCommand.getPort(), agentCommand.getStateMachine()))
                .stateMachineEntryConverter(stateMachineEntryConverter(agentCommand.getStateMachine()))
                .start();
        raftServerMono.block();*/

    }

    private RaftStorage raftStorage(AgentCommand agentCommand) {
        if (agentCommand.isDev()) {
            return new InMemoryRaftStorage();
        } else {
            String directory = agentCommand.getDataDirectory();
            return new FileSystemRaftStorage(
                    RaftStorageConfiguration.builder()
                            .segmentSize(SizeUnit.kilobytes, 8)
                            .directory(directory)
                            .build());
        }
    }

}
