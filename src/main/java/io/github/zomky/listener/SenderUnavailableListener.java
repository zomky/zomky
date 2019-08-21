package io.github.zomky.listener;

import io.github.zomky.transport.Sender;

public interface SenderUnavailableListener {

    void handle(Sender sender);

}
