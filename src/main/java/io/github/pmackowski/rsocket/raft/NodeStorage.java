package io.github.pmackowski.rsocket.raft;

import io.github.pmackowski.rsocket.raft.storage.RaftStorage;

public interface NodeStorage {

    RaftStorage openRaftStorage(String groupName);

}
