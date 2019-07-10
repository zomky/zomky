package io.github.pmackowski.rsocket.raft.external.cli.runner;

import io.github.pmackowski.rsocket.raft.cli.ZomkyCommandRunner;
import io.github.pmackowski.rsocket.raft.external.cli.command.KeyValueConfigureCommand.KeyValueAddCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.ParseResult;

import static io.github.pmackowski.rsocket.raft.external.cli.command.KeyValueConfigureCommand.KEY_VALUE_ADD_COMMAND_NAME;
import static io.github.pmackowski.rsocket.raft.external.cli.command.KeyValueConfigureCommand.KEY_VALUE_COMMAND_NAME;

public class KeyValueAddCommandRunner implements ZomkyCommandRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeyValueAddCommandRunner.class);

    @Override
    public boolean support(ParseResult parseResult) {
        return support(KEY_VALUE_COMMAND_NAME, KEY_VALUE_ADD_COMMAND_NAME, parseResult);
    }

    @Override
    public void execute(ParseResult parseResult) {
        KeyValueAddCommand command = subCommand(parseResult, KeyValueAddCommand.class);
        LOGGER.info("execute {}", command);
    }
}
