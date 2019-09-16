package io.github.zomky.raft;

import io.github.zomky.storage.log.entry.CommandEntry;

public interface StateMachine<T> {

    T applyLogEntry(CommandEntry logEntry);

}
