package io.github.pmackowski.rsocket.raft.cli.command;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "join")
public class JoinCommand {

    @Option(names = "--joinNodeId")
    private Integer joinNodeId;

    public Integer getJoinNodeId() {
        return joinNodeId;
    }
}
