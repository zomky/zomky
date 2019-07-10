package io.github.pmackowski.rsocket.raft.external.cli.command;

import io.github.pmackowski.rsocket.raft.cli.ZomkyConfigureCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

public class KeyValueConfigureCommand implements ZomkyConfigureCommand {

    public static final String KEY_VALUE_COMMAND_NAME = "kv";
    public static final String KEY_VALUE_ADD_COMMAND_NAME = "add";
    public static final String KEY_VALUE_ADD_MULTIPLE_COMMAND_NAME = "add-multiple";

    @Override
    public void configure(CommandLine commandLine) {
        commandLine.addSubcommand(KEY_VALUE_COMMAND_NAME, new CommandLine(new KeyValueCommand())
                .addSubcommand(KEY_VALUE_ADD_COMMAND_NAME, new KeyValueAddCommand())
                .addSubcommand(KEY_VALUE_ADD_MULTIPLE_COMMAND_NAME, new KeyValueAddMultipleCommand())
        );
    }

    @Command
    private static class KeyValueCommand {

    }

    @Command
    public static class KeyValueAddCommand {

        @Option(names = "--key")
        private String key;

        @Option(names = "--value")
        private String value;

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "KeyValueAddCommand{" +
                    "key='" + key + '\'' +
                    ", value='" + value + '\'' +
                    '}';
        }
    }

    @Command
    public static class KeyValueAddMultipleCommand {

        @Option(names = "-n")
        private Integer numberOfEntries = 10;

        public Integer getNumberOfEntries() {
            return numberOfEntries;
        }
    }
}


