package io.github.pmackowski.rsocket.raft.external.statemachine;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.pmackowski.rsocket.raft.raft.RaftException;
import io.github.pmackowski.rsocket.raft.raft.StateMachineEntryConverter;
import io.github.pmackowski.rsocket.raft.annotation.ZomkyStateMachineEntryConventer;
import io.github.pmackowski.rsocket.raft.external.protobuf.CommandRequest;
import io.github.pmackowski.rsocket.raft.utils.NettyUtils;
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
