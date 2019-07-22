package io.github.pmackowski.rsocket.raft.external.cli.runner;

import io.github.pmackowski.rsocket.raft.cli.ZomkyCommandRunner;
import io.github.pmackowski.rsocket.raft.cli.command.MainCommand;
import io.github.pmackowski.rsocket.raft.external.cli.command.KeyValueAddMultipleCommand;
import io.github.pmackowski.rsocket.raft.external.statemachine.KVStoreClient;
import io.github.pmackowski.rsocket.raft.external.statemachine.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.ParseResult;
import reactor.core.publisher.Flux;

import java.time.Duration;

public class KeyValueAddMultipleCommandRunner implements ZomkyCommandRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeyValueAddMultipleCommandRunner.class);

    @Override
    public boolean support(ParseResult parseResult) {
        return support("kv", "add-multiple", parseResult);
    }

    @Override
    public void execute(ParseResult parseResult) {
        MainCommand mainCommand = mainCommand(parseResult);

        KeyValueAddMultipleCommand command = subCommand(parseResult, KeyValueAddMultipleCommand.class);

        KVStoreClient kvStoreClient = new KVStoreClient(mainCommand.getAgentPort());

        int nbEntries = command.getNumberOfEntries();
        String groupName = command.getGroupName();

        kvStoreClient.put(groupName, Flux.range(1, nbEntries).delayElements(Duration.ofMillis(500)).map(i -> new KeyValue("key" + i, "val" + i)))
                .doOnSubscribe(subscription -> LOGGER.info("KVStoreClient started"))
                .doOnNext(s -> LOGGER.info("KVStoreClient received {}", s))
                .doOnComplete(() -> LOGGER.info("KVStoreClient finished"))
                .blockLast();
    }

}
