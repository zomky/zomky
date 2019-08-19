package io.github.pmackowski.rsocket.raft.cli.runner;

import io.github.pmackowski.rsocket.raft.FileSystemNodeStorage;
import io.github.pmackowski.rsocket.raft.InMemoryNodeStorage;
import io.github.pmackowski.rsocket.raft.NodeFactory;
import io.github.pmackowski.rsocket.raft.cli.ZomkyCommandRunner;
import io.github.pmackowski.rsocket.raft.cli.command.AgentCommand;
import io.github.pmackowski.rsocket.raft.cli.command.MainCommand;
import picocli.CommandLine.ParseResult;

public class AgentCommandRunner implements ZomkyCommandRunner {

    @Override
    public boolean support(ParseResult parseResult) {
        return support("agent", parseResult);
    }

    @Override
    public void execute(ParseResult parseResult) {
        MainCommand mainCommand = mainCommand(parseResult);
        AgentCommand agentCommand = command(parseResult, AgentCommand.class);

        NodeFactory.receiveExperimental()
                .storage(agentCommand.isDev() ? new InMemoryNodeStorage() : new FileSystemNodeStorage(agentCommand.getDataDirectory()))
                .nodeName(agentCommand.getNodeName())
                .port(mainCommand.getAgentPort())
                .join(agentCommand.getJoin())
                .retryJoin(agentCommand.getRetryJoin())
                .start()
                .block();

        try {
            Thread.sleep(1000000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
