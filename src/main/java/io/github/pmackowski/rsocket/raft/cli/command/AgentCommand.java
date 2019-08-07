package io.github.pmackowski.rsocket.raft.cli.command;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "agent")
public class AgentCommand {

    @Option(names = "--data-dir")
    private String dataDirectory;

    @Option(names = "--node-name")
    private String nodeName;

    @Option(names = "--dev")
    private boolean dev = false;

    @Option(names = "--join")
    private Integer join;

    @Option(names = "--retry-join")
    private Integer retryJoin;

    public String getDataDirectory() {
        return dataDirectory;
    }

    public String getNodeName() {
        return nodeName;
    }

    public boolean isDev() {
        return dev;
    }

    public Integer getJoin() {
        return join;
    }

    public Integer getRetryJoin() {
        return retryJoin;
    }
}
