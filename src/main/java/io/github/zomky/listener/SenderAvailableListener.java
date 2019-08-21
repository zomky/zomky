package io.github.zomky.listener;

import io.github.zomky.transport.Sender;

public interface SenderAvailableListener {

    void handle(Sender sender);

}
