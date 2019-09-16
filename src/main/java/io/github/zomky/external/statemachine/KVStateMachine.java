package io.github.zomky.external.statemachine;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.zomky.external.protobuf.CommandRequest;
import io.github.zomky.raft.StateMachine;
import io.github.zomky.storage.log.entry.CommandEntry;
import io.github.zomky.storage.log.entry.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class KVStateMachine implements StateMachine<ByteBuffer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KVStateMachine.class);

    private int nodeId;
    private int number;

    public KVStateMachine(Integer number, Integer nodeId) {
        this.nodeId = nodeId;
        this.number = number;
    }

    @Override
    public ByteBuffer applyLogEntry(CommandEntry entry) {
        try {
            CommandRequest commandRequest = CommandRequest.parseFrom(entry.getValue());
            LOGGER.debug("[State machine({}) {}] [PUT] {} = {}", number, nodeId, commandRequest.getKey(), commandRequest.getValue());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return ByteBuffer.wrap(entry.getValue());
    }

}
