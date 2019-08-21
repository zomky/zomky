package io.github.zomky.cli.runner;

import io.github.zomky.cli.ZomkyCommandRunner;
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
        RaftManagementClient clusterManagementClient = new RaftManagementClient(mainCommand.getAgentPort());
        clusterManagementClient.clusterInfo()
                .doOnNext(clusterInfo -> {
                    LOGGER.info("Cluster info {}", clusterInfo);
                })
                .block();*/
    }
}
