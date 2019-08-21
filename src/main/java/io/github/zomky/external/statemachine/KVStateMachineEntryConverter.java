package io.github.zomky.external.statemachine;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.zomky.annotation.ZomkyStateMachineEntryConventer;
import io.github.zomky.external.protobuf.CommandRequest;
import io.github.zomky.raft.RaftException;
import io.github.zomky.raft.StateMachineEntryConverter;
import io.github.zomky.utils.NettyUtils;
import io.rsocket.Payload;

@ZomkyStateMachineEntryConventer(name = "kv1")
public class KVStateMachineEntryConverter implements StateMachineEntryConverter {

    @Override
    public byte[] convert(Payload payload) {
        try {
            final CommandRequest commandRequest = CommandRequest.parseFrom(NettyUtils.toByteArray(payload.sliceData()));
            return commandRequest.toByteArray();
        } catch (InvalidProtocolBufferException e) {
            throw new RaftException("Invalid command request!", e);
        }
    }
}
