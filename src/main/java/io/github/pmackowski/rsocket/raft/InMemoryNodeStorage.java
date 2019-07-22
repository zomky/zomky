package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.storage.InMemoryRaftStorage;
import io.github.pmackowski.rsocket.raft.storage.RaftStorage;

public class InMemoryNodeStorage implements NodeStorage {

    @Override
    public RaftStorage openRaftStorage(String groupName) {
        return new InMemoryRaftStorage();
    }
}
