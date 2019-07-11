package io.github.pmackowski.rsocket.raft.cli;

import picocli.CommandLine;

public interface CommandLineExtension {

    void extend(CommandLine commandLine);

}
