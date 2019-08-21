package io.github.zomky.external.cli.command;

import picocli.CommandLine;

@CommandLine.Command(name = "kv", subcommands = {
        KeyValueAddCommand.class,
        KeyValueAddMultipleCommand.class
})
public class KeyValueCommand {
}
