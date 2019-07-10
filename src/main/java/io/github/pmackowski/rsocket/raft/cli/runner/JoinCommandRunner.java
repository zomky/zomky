package io.github.pmackowski.rsocket.raft.cli.runner;

import io.github.pmackowski.rsocket.raft.cli.ZomkyCommandRunner;
import io.github.pmackowski.rsocket.raft.cli.command.JoinConfigureCommand;
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
        JoinConfigureCommand.JoinCommand joinCommand = command(parseResult, JoinConfigureCommand.JoinCommand.class);
        ClusterManagementClient clusterManagementClient = new ClusterManagementClient(mainCommand.getPort());
        clusterManagementClient.addServer(joinCommand.getJoinNodeId()).block();
    }

}
