package io.github.zomky.cli.runner;

import io.github.zomky.cli.ZomkyCommandRunner;
import io.github.zomky.cli.command.LeaveCommand;
import io.github.zomky.cli.command.MainCommand;
import io.github.zomky.client.ClusterManagementClient;
import picocli.CommandLine.ParseResult;

public class LeaveCommandRunner implements ZomkyCommandRunner {

    @Override
    public boolean support(ParseResult parseResult) {
        return support("leave", parseResult);
    }

    @Override
    public void execute(ParseResult parseResult) {
        MainCommand mainCommand = mainCommand(parseResult);
        LeaveCommand leaveCommand = command(parseResult, LeaveCommand.class);
        ClusterManagementClient clusterManagementClient = new ClusterManagementClient();
        clusterManagementClient.initLeave(mainCommand.getAgentPort()).block();
    }
}
