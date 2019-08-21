package io.github.zomky.cli;

import io.github.zomky.cli.command.MainCommand;
import io.github.zomky.utils.ReflectionsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.util.List;

public class Zomky {

    private static final Logger LOGGER = LoggerFactory.getLogger(Zomky.class);

    public static void main(String argv[]) {
        // extend command line
        MainCommand mainCommand = new MainCommand();
        CommandLine commandLine = new CommandLine(mainCommand);
        List<CommandLineExtension> commandLineExtensions = ReflectionsUtils.getSubTypesOf(CommandLineExtension.class);
        commandLineExtensions.forEach(commandLineExtension -> commandLineExtension.extend(commandLine));

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

        if (parseResult.subcommand() == null) {
            LOGGER.error("Command is required!");
            return;
        }

        List<ZomkyCommandRunner> commandRunners = ReflectionsUtils.getSubTypesOf(ZomkyCommandRunner.class);
        commandRunners.stream()
            .filter(c -> c.support(parseResult))
            .forEach(commandRunner -> commandRunner.execute(parseResult));
    }

}
