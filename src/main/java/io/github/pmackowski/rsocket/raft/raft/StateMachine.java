package io.github.pmackowski.rsocket.raft.raft;

import io.github.pmackowski.rsocket.raft.storage.log.entry.LogEntry;

public interface StateMachine<T> {

    T applyLogEntry(LogEntry logEntry);

}
