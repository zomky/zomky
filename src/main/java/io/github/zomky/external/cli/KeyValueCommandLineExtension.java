package io.github.zomky.external.cli;

import io.github.zomky.cli.CommandLineExtension;
import io.github.zomky.external.cli.command.KeyValueCommand;
import picocli.CommandLine;

public class KeyValueCommandLineExtension implements CommandLineExtension {

    @Override
    public void extend(CommandLine commandLine) {
        commandLine.addSubcommand("kv", new CommandLine(new KeyValueCommand()));
    }

}

