package io.github.pmackowski.rsocket.raft.cli.command;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "agent")
public class AgentCommand {

    @Option(names = "--state-machine", required = true)
    private String stateMachine;

    @Option(names = "--data-directory")
    private String dataDirectory;

    @Option(names = {"--passive", "-p"})
    private boolean passive = false;

    @Option(names = {"--preVote"}, hidden = true)
    private boolean preVote = true;

    @Option(names = {"--leaderStickiness"})
    private boolean leaderStickiness = true;

    @Option(names = "--joinNodeId")
    private Integer joinNodeId;

    @Option(names = "--key")
    private String key;

    @Option(names = "--value")
    private String value;

    @Option(names = "--dev")
    private boolean dev = false;


    public String getDataDirectory() {
        return dataDirectory;
    }

    public boolean isPassive() {
        return passive;
    }

    public boolean isPreVote() {
        return preVote;
    }

    public boolean isLeaderStickiness() {
        return leaderStickiness;
    }

    public Integer getJoinNodeId() {
        return joinNodeId;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public boolean isDev() {
        return dev;
    }

    public String getStateMachine() {
        return stateMachine;
    }

}
