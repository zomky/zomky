package io.github.zomky.cli.runner;

import io.github.zomky.cli.ZomkyCommandRunner;
import io.github.zomky.cli.command.JoinCommand;
import io.github.zomky.cli.command.MainCommand;
import io.github.zomky.client.ClusterManagementClient;
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
        clusterManagementClient.initJoin(mainCommand.getAgentPort(), joinCommand.getHost(), joinCommand.getPort()).block();
    }

}
