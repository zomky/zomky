package io.github.pmackowski.rsocket.raft.external.cli;

import io.github.pmackowski.rsocket.raft.cli.CommandLineExtension;
import io.github.pmackowski.rsocket.raft.external.cli.command.KeyValueCommand;
import picocli.CommandLine;

public class KeyValueCommandLineExtension implements CommandLineExtension {

    @Override
    public void extend(CommandLine commandLine) {
        commandLine.addSubcommand("kv", new CommandLine(new KeyValueCommand()));
    }

}

