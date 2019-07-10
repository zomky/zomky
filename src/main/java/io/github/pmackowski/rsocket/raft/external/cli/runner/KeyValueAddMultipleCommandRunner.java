package io.github.pmackowski.rsocket.raft.external.cli.runner;

import io.github.pmackowski.rsocket.raft.cli.ZomkyCommandRunner;
import io.github.pmackowski.rsocket.raft.cli.command.MainCommand;
import io.github.pmackowski.rsocket.raft.external.cli.command.KeyValueConfigureCommand.KeyValueAddMultipleCommand;
import io.github.pmackowski.rsocket.raft.external.statemachine.KVStoreClient;
import io.github.pmackowski.rsocket.raft.external.statemachine.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.ParseResult;
import reactor.core.publisher.Flux;

import java.time.Duration;

import static io.github.pmackowski.rsocket.raft.external.cli.command.KeyValueConfigureCommand.KEY_VALUE_ADD_MULTIPLE_COMMAND_NAME;
import static io.github.pmackowski.rsocket.raft.external.cli.command.KeyValueConfigureCommand.KEY_VALUE_COMMAND_NAME;

public class KeyValueAddMultipleCommandRunner implements ZomkyCommandRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeyValueAddMultipleCommandRunner.class);

    @Override
    public boolean support(ParseResult parseResult) {
        return support(KEY_VALUE_COMMAND_NAME, KEY_VALUE_ADD_MULTIPLE_COMMAND_NAME, parseResult);
    }

    @Override
    public void execute(ParseResult parseResult) {
        MainCommand mainCommand = mainCommand(parseResult);

        KeyValueAddMultipleCommand command = subCommand(parseResult, KeyValueAddMultipleCommand.class);

        KVStoreClient kvStoreClient = new KVStoreClient(mainCommand.getPort());

        int nbEntries = command.getNumberOfEntries();

        kvStoreClient.put(Flux.range(1, nbEntries).delayElements(Duration.ofMillis(500)).map(i -> new KeyValue("key" + i, "val" + i)))
                .doOnSubscribe(subscription -> LOGGER.info("KVStoreClient started"))
                .doOnNext(s -> LOGGER.info("KVStoreClient received {}", s))
                .doOnComplete(() -> LOGGER.info("KVStoreClient finished"))
                .blockLast();
    }

}
