package io.github.pmackowski.rsocket.raft.external.cli.command;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "add-multiple")
public class KeyValueAddMultipleCommand {

    @Option(names = "-n")
    private Integer numberOfEntries = 10;

    @Option(names = "-g")
    private String groupName;

    public Integer getNumberOfEntries() {
        return numberOfEntries;
    }

    public String getGroupName() {
        return groupName;
    }
}
