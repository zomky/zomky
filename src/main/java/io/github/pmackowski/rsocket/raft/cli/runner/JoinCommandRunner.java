package io.github.pmackowski.rsocket.raft.cli.runner;

import io.github.pmackowski.rsocket.raft.cli.ZomkyCommandRunner;
import io.github.pmackowski.rsocket.raft.cli.command.JoinCommand;
import io.github.pmackowski.rsocket.raft.cli.command.MainCommand;
import io.github.pmackowski.rsocket.raft.client.ClusterManagementClient;
import picocli.CommandLine.ParseResult;

public class JoinCommandRunner implements ZomkyCommandRunner {

    @Override
    public boolean support(ParseResult parseResult) {
        return support("join", parseResult);
    }

    @Override
    public void execute(ParseResult parseResult) {
        MainCommand mainCommand = mainCommand(parseResult);
        JoinCommand joinCommand = command(parseResult, JoinCommand.class);
        ClusterManagementClient clusterManagementClient = new ClusterManagementClient();
        clusterManagementClient.join(mainCommand.getAgentPort(), joinCommand.getHost(), joinCommand.getPort()).block();
    }

}
