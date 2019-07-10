package io.github.pmackowski.rsocket.raft.external.statemachine;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.pmackowski.rsocket.raft.StateMachine;
import io.github.pmackowski.rsocket.raft.storage.log.entry.CommandEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.LogEntry;
import io.github.pmackowski.rsocket.raft.transport.protobuf.CommandRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class KVStateMachine implements StateMachine<ByteBuffer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KVStateMachine1.class);

    private int nodeId;
    private int number;

    public KVStateMachine(Integer number, Integer nodeId) {
        this.nodeId = nodeId;
        this.number = number;
    }

    @Override
    public ByteBuffer applyLogEntry(LogEntry entry) {
        try {
            CommandRequest commandRequest = CommandRequest.parseFrom(((CommandEntry) entry).getValue());
            LOGGER.info("[State machine({}) {}] [PUT] {} = {}", number, nodeId, commandRequest.getKey(), commandRequest.getValue());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return ByteBuffer.wrap(((CommandEntry) entry).getValue());
    }

}
