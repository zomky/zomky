// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: Rpc.proto

package rsocket.playground.raft.rpc;

public final class Rpc {
  private Rpc() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_VoteRequest_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_VoteRequest_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_VoteResponse_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_VoteResponse_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_AppendEntriesRequest_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_AppendEntriesRequest_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_AppendEntriesResponse_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_AppendEntriesResponse_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_InstallSnapshotRequest_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_InstallSnapshotRequest_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_InstallSnapshotResponse_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_InstallSnapshotResponse_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\tRpc.proto\"`\n\013VoteRequest\022\014\n\004term\030\001 \001(\005" +
      "\022\024\n\014candidate_id\030\002 \001(\005\022\026\n\016last_log_index" +
      "\030\003 \001(\003\022\025\n\rlast_log_term\030\004 \001(\003\"2\n\014VoteRes" +
      "ponse\022\014\n\004term\030\001 \001(\005\022\024\n\014vote_granted\030\002 \001(" +
      "\010\"\235\001\n\024AppendEntriesRequest\022\014\n\004term\030\001 \001(\005" +
      "\022\020\n\010leaderId\030\002 \001(\005\022\024\n\014prevLogIndex\030\003 \001(\003" +
      "\022\023\n\013prevLogTerm\030\004 \001(\003\022\017\n\007entries\030\005 \001(\014\022\023" +
      "\n\013entriesSize\030\006 \001(\005\022\024\n\014leaderCommit\030\007 \001(" +
      "\003\"6\n\025AppendEntriesResponse\022\014\n\004term\030\001 \001(\005" +
      "\022\017\n\007success\030\002 \001(\010\"\211\001\n\026InstallSnapshotReq",
      "uest\022\014\n\004term\030\001 \001(\005\022\020\n\010leaderId\030\002 \001(\005\022\031\n\021" +
      "lastIncludedIndex\030\003 \001(\003\022\030\n\020lastIncludedT" +
      "erm\030\004 \001(\003\022\014\n\004data\030\005 \001(\014\022\014\n\004done\030\006 \001(\010\"\'\n" +
      "\027InstallSnapshotResponse\022\014\n\004term\030\001 \001(\005B\037" +
      "\n\033rsocket.playground.raft.rpcP\001b\006proto3"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
        new com.google.protobuf.Descriptors.FileDescriptor.    InternalDescriptorAssigner() {
          public com.google.protobuf.ExtensionRegistry assignDescriptors(
              com.google.protobuf.Descriptors.FileDescriptor root) {
            descriptor = root;
            return null;
          }
        };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
    internal_static_VoteRequest_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_VoteRequest_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_VoteRequest_descriptor,
        new java.lang.String[] { "Term", "CandidateId", "LastLogIndex", "LastLogTerm", });
    internal_static_VoteResponse_descriptor =
      getDescriptor().getMessageTypes().get(1);
    internal_static_VoteResponse_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_VoteResponse_descriptor,
        new java.lang.String[] { "Term", "VoteGranted", });
    internal_static_AppendEntriesRequest_descriptor =
      getDescriptor().getMessageTypes().get(2);
    internal_static_AppendEntriesRequest_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_AppendEntriesRequest_descriptor,
        new java.lang.String[] { "Term", "LeaderId", "PrevLogIndex", "PrevLogTerm", "Entries", "EntriesSize", "LeaderCommit", });
    internal_static_AppendEntriesResponse_descriptor =
      getDescriptor().getMessageTypes().get(3);
    internal_static_AppendEntriesResponse_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_AppendEntriesResponse_descriptor,
        new java.lang.String[] { "Term", "Success", });
    internal_static_InstallSnapshotRequest_descriptor =
      getDescriptor().getMessageTypes().get(4);
    internal_static_InstallSnapshotRequest_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_InstallSnapshotRequest_descriptor,
        new java.lang.String[] { "Term", "LeaderId", "LastIncludedIndex", "LastIncludedTerm", "Data", "Done", });
    internal_static_InstallSnapshotResponse_descriptor =
      getDescriptor().getMessageTypes().get(5);
    internal_static_InstallSnapshotResponse_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_InstallSnapshotResponse_descriptor,
        new java.lang.String[] { "Term", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}