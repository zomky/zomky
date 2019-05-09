package rsocket.playground.raft.statemachine.kv;

import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rsocket.playground.raft.StateMachine;
import rsocket.playground.raft.rpc.CommandRequest;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class KVStateMachine implements StateMachine {

    Map<String,String> map = new HashMap<>();

    private static final Logger LOGGER = LoggerFactory.getLogger(KVStateMachine.class);

    private int nodeId;

    public KVStateMachine(int nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public ByteBuffer applyLogEntry(ByteBuffer entry) {
        try {
            CommandRequest commandRequest = CommandRequest.parseFrom(entry.array());
            LOGGER.debug("[KVStoreServerImpl {}] [PUT] {} = {}", nodeId, commandRequest.getKey(), commandRequest.getValue());
            map.put(commandRequest.getKey(), commandRequest.getValue());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
//      String req = new String(entry.array());
//      LOGGER.info("[KVStoreServerImpl {}] APPLY {}", nodeId, req);
//      return ByteBuffer.wrap((req + "-resp").getBytes());
        return ByteBuffer.wrap(entry.array());

    }

}
