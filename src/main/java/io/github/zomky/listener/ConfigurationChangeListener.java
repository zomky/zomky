package io.github.zomky.listener;

import io.github.zomky.storage.meta.Configuration;

public interface ConfigurationChangeListener {

    void handle(Configuration oldConfiguration, Configuration newConfiguration);

}
