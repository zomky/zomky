package io.github.pmackowski.rsocket.raft.kvstore;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.pmackowski.rsocket.raft.StateMachine;
import io.github.pmackowski.rsocket.raft.external.protobuf.CommandRequest;
import io.github.pmackowski.rsocket.raft.storage.log.entry.CommandEntry;
import io.github.pmackowski.rsocket.raft.storage.log.entry.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class KVStateMachine implements StateMachine<ByteBuffer> {

    Map<String,String> map = new HashMap<>();

    private static final Logger LOGGER = LoggerFactory.getLogger(KVStateMachine.class);

    private int nodeId;

    public KVStateMachine(int nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public ByteBuffer applyLogEntry(LogEntry entry) {
        try {
            CommandRequest commandRequest = CommandRequest.parseFrom(((CommandEntry) entry).getValue());
            LOGGER.debug("[KVStoreServerImpl {}] [PUT] {} = {}", nodeId, commandRequest.getKey(), commandRequest.getValue());
            map.put(commandRequest.getKey(), commandRequest.getValue());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
//      String req = new String(entry.array());
//      LOGGER.info("[KVStoreServerImpl {}] APPLY {}", nodeId, req);
//      return ByteBuffer.wrap((req + "-resp").getBytes());
        return ByteBuffer.wrap(((CommandEntry) entry).getValue());

    }

}
