package io.github.zomky.utils;

import org.reflections.Reflections;

import java.util.List;
import java.util.stream.Collectors;

public class ReflectionsUtils {

    public static <T> List<T> getSubTypesOf(Class<T> type) {
        Reflections reflections = new Reflections("io.github.zomky"); // TODO
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
