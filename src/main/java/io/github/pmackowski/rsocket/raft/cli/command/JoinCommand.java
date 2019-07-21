package io.github.pmackowski.rsocket.raft.cli.command;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.net.InetAddress;

@Command(name = "join")
public class JoinCommand {

    @Parameters(index = "0")
    InetAddress host;
    @Parameters(index = "1")
    int port;

    public InetAddress getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
