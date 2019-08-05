package io.github.pmackowski.rsocket.raft.cli.runner;

import io.github.pmackowski.rsocket.raft.cli.ZomkyCommandRunner;
import io.github.pmackowski.rsocket.raft.cli.command.LeaveCommand;
import io.github.pmackowski.rsocket.raft.cli.command.MainCommand;
import io.github.pmackowski.rsocket.raft.client.ClusterManagementClient;
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
        clusterManagementClient.leave(mainCommand.getAgentPort()).block();
    }
}
