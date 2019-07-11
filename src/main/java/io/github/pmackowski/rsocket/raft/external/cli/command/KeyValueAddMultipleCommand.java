package io.github.pmackowski.rsocket.raft.external.cli.command;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "add-multiple")
public class KeyValueAddMultipleCommand {

    @Option(names = "-n")
    private Integer numberOfEntries = 10;

    public Integer getNumberOfEntries() {
        return numberOfEntries;
    }

}
