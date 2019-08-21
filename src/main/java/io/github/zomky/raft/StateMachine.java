package io.github.zomky.raft;

import io.github.zomky.annotation.ZomkyStateMachine;
import io.github.zomky.annotation.ZomkyStateMachineEntryConventer;
import io.github.zomky.storage.log.entry.LogEntry;
import org.reflections.Reflections;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Set;

public interface StateMachine<T> {

    T applyLogEntry(LogEntry logEntry);

    // TODO move somewhere else
    static StateMachine stateMachine(int port, String stateMachineName) {
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

    // TODO move somewhere else
    static StateMachineEntryConverter stateMachineEntryConverter(String stateMachineName) {
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
}
