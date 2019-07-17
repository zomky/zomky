package io.github.pmackowski.rsocket.raft.cli.runner;

import io.github.pmackowski.rsocket.raft.cli.ZomkyCommandRunner;
import io.github.pmackowski.rsocket.raft.cli.command.MainCommand;
import io.github.pmackowski.rsocket.raft.client.ClusterManagementClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

public class InfoCommandRunner implements ZomkyCommandRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(InfoCommandRunner.class);

    public boolean support(CommandLine.ParseResult parseResult) {
        return support("info", parseResult);
    }

    @Override
    public void execute(CommandLine.ParseResult parseResult) {
        /*MainCommand mainCommand = mainCommand(parseResult);
        ClusterManagementClient clusterManagementClient = new ClusterManagementClient(mainCommand.getPort());
        clusterManagementClient.clusterInfo()
                .doOnNext(clusterInfo -> {
                    LOGGER.info("Cluster info {}", clusterInfo);
                })
                .block();*/
    }
}
