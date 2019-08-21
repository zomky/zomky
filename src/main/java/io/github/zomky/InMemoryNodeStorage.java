package io.github.zomky;

import io.github.zomky.storage.InMemoryRaftStorage;
import io.github.zomky.storage.RaftStorage;

public class InMemoryNodeStorage implements NodeStorage {

    @Override
    public RaftStorage openRaftStorage(String groupName) {
        return new InMemoryRaftStorage();
    }
}
