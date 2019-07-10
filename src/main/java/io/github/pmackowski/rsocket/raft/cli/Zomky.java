package io.github.pmackowski.rsocket.raft.cli;

import io.github.pmackowski.rsocket.raft.cli.command.MainCommand;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.util.List;
import java.util.stream.Collectors;

public class Zomky {

    private static final Logger LOGGER = LoggerFactory.getLogger(Zomky.class);

    // Usage: zomky [--version] [--help] [--port port] <runner> [<args>]
    public static void main(String argv[]) {
        // configure
        MainCommand mainCommand = new MainCommand();
        CommandLine commandLine = new CommandLine(mainCommand);
        List<ZomkyConfigureCommand> zomkyConfigureCommands = findAllConfigureCommands();
        zomkyConfigureCommands.forEach(zomkyConfigureCommand -> zomkyConfigureCommand.configure(commandLine));

        // parse
        CommandLine.ParseResult parseResult = commandLine.parseArgs(argv);

        // execute
        if (commandLine.isUsageHelpRequested()) {
            commandLine.usage(System.out);
            return;
        } else if (commandLine.isVersionHelpRequested()) {
            commandLine.printVersionHelp(System.out);
            return;
        }

        CommandLine.ParseResult subcommand = parseResult.subcommand();
        if (subcommand == null) {
            LOGGER.error("Command is required!");
            return;
        }

        List<ZomkyCommandRunner> commandRunners = findAllCommands();
        commandRunners.stream()
            .filter(c -> c.support(parseResult))
            .forEach(commandRunner -> {
                commandRunner.execute(parseResult);
            });
    }

    private static List<ZomkyConfigureCommand> findAllConfigureCommands() {
        return getSubTypesOf(ZomkyConfigureCommand.class);
    }

    private static List<ZomkyCommandRunner> findAllCommands() {
        return getSubTypesOf(ZomkyCommandRunner.class);
    }

    private static <T> List<T> getSubTypesOf(Class<T> type) {
        Reflections reflections = new Reflections("io.github.pmackowski"); // TODO
        return reflections.getSubTypesOf(type).stream()
                .map(type1 -> {
                    try {
                        return type1.newInstance();
                    } catch (InstantiationException | IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());
    }
}
