// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: Rpc.proto

package io.github.pmackowski.rsocket.raft.rpc;

public interface AppendEntriesResponseOrBuilder extends
    // @@protoc_insertion_point(interface_extends:AppendEntriesResponse)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <pre>
   * currentTerm, for leader to update itself
   * </pre>
   *
   * <code>optional int32 term = 1;</code>
   */
  int getTerm();

  /**
   * <code>optional bool success = 2;</code>
   */
  boolean getSuccess();

  /**
   * <pre>
   * last log index, set only if success equals to false (extension to Raft)
   * </pre>
   *
   * <code>optional int64 last_log_index = 3;</code>
   */
  long getLastLogIndex();
}
