// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: Rpc.proto

package rsocket.playground.raft.rpc;

public interface InstallSnapshotResponseOrBuilder extends
    // @@protoc_insertion_point(interface_extends:InstallSnapshotResponse)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <pre>
   * currentTerm, for leader to update itself
   * </pre>
   *
   * <code>optional int32 term = 1;</code>
   */
  int getTerm();
}