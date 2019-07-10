package io.github.pmackowski.rsocket.raft.cli.command;

import io.github.pmackowski.rsocket.raft.cli.ZomkyConfigureCommand;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class JoinConfigureCommand implements ZomkyConfigureCommand {

    @Override
    public void configure(CommandLine commandLine) {
        commandLine.addSubcommand("join", new JoinCommand());
    }

    public class JoinCommand {

        @Option(names = "--joinNodeId")
        private Integer joinNodeId;

        public JoinCommand() {
        }

        public Integer getJoinNodeId() {
            return joinNodeId;
        }
    }
}
