package io.github.pmackowski.rsocket.raft.listener;

import io.github.pmackowski.rsocket.raft.storage.meta.Configuration;

public interface ConfigurationChangeListener {

    void handle(Configuration oldConfiguration, Configuration newConfiguration);

}
