package io.github.zomky.cli;

import picocli.CommandLine;

public interface CommandLineExtension {

    void extend(CommandLine commandLine);

}
