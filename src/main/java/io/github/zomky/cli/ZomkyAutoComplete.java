package io.github.zomky.cli;

import io.github.zomky.cli.command.MainCommand;
import io.github.zomky.utils.ReflectionsUtils;
import picocli.AutoComplete;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class ZomkyAutoComplete {

    public static void main(String argv[]) throws IOException {
        MainCommand mainCommand = new MainCommand();
        CommandLine commandLine = new CommandLine(mainCommand);
        List<CommandLineExtension> commandLineExtensions = ReflectionsUtils.getSubTypesOf(CommandLineExtension.class);
        commandLineExtensions.forEach(commandLineExtension -> commandLineExtension.extend(commandLine));

        String commandName = commandLine.getCommandName();
        File autoCompleteScript = new File(commandName + "_completion");

        AutoComplete.bash(commandName, autoCompleteScript, null, commandLine);
    }

}