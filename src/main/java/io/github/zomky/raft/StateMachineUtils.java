package io.github.zomky.raft;

import io.github.zomky.annotation.ZomkyStateMachine;
import io.github.zomky.annotation.ZomkyStateMachineEntryConventer;
import org.reflections.Reflections;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class StateMachineUtils {

    static final String IO_GITHUB_ZOMKY = "io.github.zomky";

    static Map<String, StateMachine> stateMachines(int port) {
        Map<String, StateMachine> result = new HashMap<>();
        Reflections reflections = new Reflections(IO_GITHUB_ZOMKY);
        reflections.getSubTypesOf(StateMachine.class).forEach(type -> {
            ZomkyStateMachine zomkyStateMachine = type.getAnnotation(ZomkyStateMachine.class);
            if (zomkyStateMachine != null) {
                try {
                    Constructor constructor = type.getConstructor(Integer.class);
                    StateMachine stateMachine = (StateMachine) constructor.newInstance(port);
                    result.put(zomkyStateMachine.name(), stateMachine);
                } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        return result;
    }

    public static Map<String, StateMachineEntryConverter> stateMachineConverters() {
        Map<String, StateMachineEntryConverter> result = new HashMap<>();
        Reflections reflections = new Reflections(IO_GITHUB_ZOMKY);
        reflections.getSubTypesOf(StateMachineEntryConverter.class).forEach(type -> {
            ZomkyStateMachineEntryConventer zomkyStateMachineEntryConventer = type.getAnnotation(ZomkyStateMachineEntryConventer.class);
            try {
                StateMachineEntryConverter stateMachineEntryConverter = type.newInstance();
                result.put(zomkyStateMachineEntryConventer.name(), stateMachineEntryConverter);
            } catch (InstantiationException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        });
        return result;
    }

}
