package io.github.zomky.cli.runner;

import io.github.zomky.FileSystemNodeStorage;
import io.github.zomky.InMemoryNodeStorage;
import io.github.zomky.NodeFactory;
import io.github.zomky.cli.ZomkyCommandRunner;
import io.github.zomky.cli.command.AgentCommand;
import io.github.zomky.cli.command.MainCommand;
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
