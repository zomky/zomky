package io.github.zomky.external.cli.command;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "add")
public class KeyValueAddCommand {

    @Option(names = "--key")
    private String key;

    @Option(names = "--value")
    private String value;

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "KeyValueAddCommand{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }

}
