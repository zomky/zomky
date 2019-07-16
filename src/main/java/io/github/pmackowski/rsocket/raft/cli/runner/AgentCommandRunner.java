package io.github.pmackowski.rsocket.raft.cli.runner;

import io.github.pmackowski.rsocket.raft.*;
import io.github.pmackowski.rsocket.raft.annotation.ZomkyStateMachine;
import io.github.pmackowski.rsocket.raft.annotation.ZomkyStateMachineEntryConventer;
import io.github.pmackowski.rsocket.raft.cli.ZomkyCommandRunner;
import io.github.pmackowski.rsocket.raft.cli.command.AgentCommand;
import io.github.pmackowski.rsocket.raft.cli.command.MainCommand;
import io.github.pmackowski.rsocket.raft.storage.FileSystemRaftStorage;
import io.github.pmackowski.rsocket.raft.storage.InMemoryRaftStorage;
import io.github.pmackowski.rsocket.raft.storage.RaftStorage;
import io.github.pmackowski.rsocket.raft.storage.RaftStorageConfiguration;
import io.github.pmackowski.rsocket.raft.storage.log.SizeUnit;
import io.github.pmackowski.rsocket.raft.storage.meta.Configuration;
import org.reflections.Reflections;
import picocli.CommandLine.ParseResult;
import reactor.core.publisher.Mono;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Set;

public class AgentCommandRunner implements ZomkyCommandRunner {

    @Override
    public boolean support(ParseResult parseResult) {
        return support("agent", parseResult);
    }

    @Override
    public void execute(ParseResult parseResult) {
/*        MainCommand mainCommand = mainCommand(parseResult);
        AgentCommand agentCommand = command(parseResult, AgentCommand.class);

        Mono<Node> raftServerMono = new NodeFactory()
                .storage(raftStorage(agentCommand))
                .leaderStickiness(agentCommand.isLeaderStickiness())
                .preVote(agentCommand.isPreVote())
                .nodeId(mainCommand.getPort())
                .passive(agentCommand.isPassive())
                .electionTimeout(new ElectionTimeout())
                .initialConfiguration(new Configuration(mainCommand.getPort()))
                .stateMachine(stateMachine(mainCommand.getPort(), agentCommand.getStateMachine()))
                .stateMachineEntryConverter(stateMachineEntryConverter(agentCommand.getStateMachine()))
                .start();
        raftServerMono.block();*/

    }

    private StateMachine stateMachine(int port, String stateMachineName) {
        Reflections reflections = new Reflections("io.github.pmackowski"); // TODO
        Set<Class<? extends StateMachine>> stateMachineTypes = reflections.getSubTypesOf(StateMachine.class);
        Class<? extends StateMachine> stateMachineType = stateMachineTypes.stream()
                .filter(type -> {
                    ZomkyStateMachine annotation = type.getAnnotation(ZomkyStateMachine.class);
                    return annotation != null && annotation.name().equals(stateMachineName);
                })
                .findFirst()
                .orElseThrow(() -> new RuntimeException(String.format("no state machine %s", stateMachineName)));

        try {
            Constructor constructor = stateMachineType.getConstructor(Integer.class);
            return (StateMachine) constructor.newInstance(port);
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private StateMachineEntryConverter stateMachineEntryConverter(String stateMachineName) {
        Reflections reflections = new Reflections("io.github.pmackowski"); // TODO
        Set<Class<? extends StateMachineEntryConverter>> converterTypes = reflections.getSubTypesOf(StateMachineEntryConverter.class);
        Class<? extends StateMachineEntryConverter> converter = converterTypes.stream()
                .filter(type -> {
                    ZomkyStateMachineEntryConventer annotation = type.getAnnotation(ZomkyStateMachineEntryConventer.class);
                    return annotation != null && annotation.name().equals(stateMachineName);
                })
                .findFirst()
                .orElseThrow(() -> new RuntimeException(String.format("no state machine %s", stateMachineName)));

        try {
            return converter.newInstance();
        } catch (InstantiationException | IllegalAccessException  e) {
            throw new RuntimeException(e);
        }
    }

    private RaftStorage raftStorage(AgentCommand agentCommand) {
        if (agentCommand.isDev()) {
            return new InMemoryRaftStorage();
        } else {
            String directory = agentCommand.getDataDirectory();
            return new FileSystemRaftStorage(
                    RaftStorageConfiguration.builder()
                            .segmentSize(SizeUnit.kilobytes, 8)
                            .directory(directory)
                            .build());
        }
    }

}
