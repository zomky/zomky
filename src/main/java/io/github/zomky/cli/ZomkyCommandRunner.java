package io.github.zomky.cli;

import io.github.zomky.cli.command.MainCommand;
import picocli.CommandLine.ParseResult;

public interface ZomkyCommandRunner {

    boolean support(ParseResult parseResult);

    void execute(ParseResult parseResult);

    default boolean support(String commandName, ParseResult parseResult) {
        return commandName.equals(commandName(parseResult));
    }

    default boolean support(String commandName, String subCommandName, ParseResult parseResult) {
        return commandName.equals(commandName(parseResult)) &&
                subCommandName.equals(subCommandName(parseResult));
    }

    default MainCommand mainCommand(ParseResult parseResult) {
        return  (MainCommand) parseResult.asCommandLineList().get(0).getCommand();
    }

    default <T> T command(ParseResult parseResult, Class<T> type) {
        return type.cast(parseResult.asCommandLineList().get(1).getCommand());
    }

    default <T> T subCommand(ParseResult parseResult, Class<T> type) {
        return type.cast(parseResult.asCommandLineList().get(2).getCommand());
    }

    default String commandName(ParseResult parseResult) {
        return parseResult.asCommandLineList().get(1).getCommandName();
    }

    default String subCommandName(ParseResult parseResult) {
        return parseResult.asCommandLineList().get(2).getCommandName();
    }
}
