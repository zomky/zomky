package io.github.zomky.raft;

import io.github.zomky.storage.log.entry.LogEntry;

public interface StateMachine<T> {

    T applyLogEntry(LogEntry logEntry);

}
