package io.github.pmackowski.rsocket.raft.cli;

import picocli.CommandLine;

public interface ZomkyConfigureCommand {

    void configure(CommandLine commandLine);

}
