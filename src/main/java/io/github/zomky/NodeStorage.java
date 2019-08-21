package io.github.zomky;

import io.github.zomky.storage.RaftStorage;

public interface NodeStorage {

    RaftStorage openRaftStorage(String groupName);

}
