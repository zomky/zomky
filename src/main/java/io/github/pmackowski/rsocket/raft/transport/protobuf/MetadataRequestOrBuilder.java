// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: raft.proto

package io.github.pmackowski.rsocket.raft.transport.protobuf;

public interface MetadataRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:MetadataRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional int32 message_type = 1;</code>
   */
  int getMessageType();

  /**
   * <code>optional string group_name = 2;</code>
   */
  java.lang.String getGroupName();
  /**
   * <code>optional string group_name = 2;</code>
   */
  com.google.protobuf.ByteString
      getGroupNameBytes();
}