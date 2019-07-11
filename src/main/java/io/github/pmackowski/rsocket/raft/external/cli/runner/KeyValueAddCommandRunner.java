package io.github.pmackowski.rsocket.raft.external.cli.runner;

import io.github.pmackowski.rsocket.raft.cli.ZomkyCommandRunner;
import io.github.pmackowski.rsocket.raft.external.cli.command.KeyValueAddCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.ParseResult;

public class KeyValueAddCommandRunner implements ZomkyCommandRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeyValueAddCommandRunner.class);

    @Override
    public boolean support(ParseResult parseResult) {
        return support("kv", "add", parseResult);
    }

    @Override
    public void execute(ParseResult parseResult) {
        KeyValueAddCommand command = subCommand(parseResult, KeyValueAddCommand.class);
        LOGGER.info("execute {}", command);
    }
}
