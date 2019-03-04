// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: Rpc.proto

package rsocket.playground.raft.rpc;

/**
 * Protobuf type {@code InstallSnapshotRequest}
 */
public  final class InstallSnapshotRequest extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:InstallSnapshotRequest)
    InstallSnapshotRequestOrBuilder {
  // Use InstallSnapshotRequest.newBuilder() to construct.
  private InstallSnapshotRequest(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private InstallSnapshotRequest() {
    term_ = 0;
    leaderId_ = 0;
    lastIncludedIndex_ = 0L;
    lastIncludedTerm_ = 0L;
    data_ = com.google.protobuf.ByteString.EMPTY;
    done_ = false;
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return com.google.protobuf.UnknownFieldSet.getDefaultInstance();
  }
  private InstallSnapshotRequest(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    this();
    int mutable_bitField0_ = 0;
    try {
      boolean done = false;
      while (!done) {
        int tag = input.readTag();
        switch (tag) {
          case 0:
            done = true;
            break;
          default: {
            if (!input.skipField(tag)) {
              done = true;
            }
            break;
          }
          case 8: {

            term_ = input.readInt32();
            break;
          }
          case 16: {

            leaderId_ = input.readInt32();
            break;
          }
          case 24: {

            lastIncludedIndex_ = input.readInt64();
            break;
          }
          case 32: {

            lastIncludedTerm_ = input.readInt64();
            break;
          }
          case 42: {

            data_ = input.readBytes();
            break;
          }
          case 48: {

            done_ = input.readBool();
            break;
          }
        }
      }
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw e.setUnfinishedMessage(this);
    } catch (java.io.IOException e) {
      throw new com.google.protobuf.InvalidProtocolBufferException(
          e).setUnfinishedMessage(this);
    } finally {
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return rsocket.playground.raft.rpc.Rpc.internal_static_InstallSnapshotRequest_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return rsocket.playground.raft.rpc.Rpc.internal_static_InstallSnapshotRequest_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            rsocket.playground.raft.rpc.InstallSnapshotRequest.class, rsocket.playground.raft.rpc.InstallSnapshotRequest.Builder.class);
  }

  public static final int TERM_FIELD_NUMBER = 1;
  private int term_;
  /**
   * <pre>
   * leader’s term
   * </pre>
   *
   * <code>optional int32 term = 1;</code>
   */
  public int getTerm() {
    return term_;
  }

  public static final int LEADERID_FIELD_NUMBER = 2;
  private int leaderId_;
  /**
   * <pre>
   * so follower can redirect clients
   * </pre>
   *
   * <code>optional int32 leaderId = 2;</code>
   */
  public int getLeaderId() {
    return leaderId_;
  }

  public static final int LASTINCLUDEDINDEX_FIELD_NUMBER = 3;
  private long lastIncludedIndex_;
  /**
   * <pre>
   * the snapshot replaces all entries up through and including this index
   * </pre>
   *
   * <code>optional int64 lastIncludedIndex = 3;</code>
   */
  public long getLastIncludedIndex() {
    return lastIncludedIndex_;
  }

  public static final int LASTINCLUDEDTERM_FIELD_NUMBER = 4;
  private long lastIncludedTerm_;
  /**
   * <pre>
   * lastIncludedTerm term of lastIncludedIndex offset byte offset where chunk is positioned in the snapshot file
   * </pre>
   *
   * <code>optional int64 lastIncludedTerm = 4;</code>
   */
  public long getLastIncludedTerm() {
    return lastIncludedTerm_;
  }

  public static final int DATA_FIELD_NUMBER = 5;
  private com.google.protobuf.ByteString data_;
  /**
   * <pre>
   * raw bytes of the snapshot chunk, starting at offset
   * </pre>
   *
   * <code>optional bytes data = 5;</code>
   */
  public com.google.protobuf.ByteString getData() {
    return data_;
  }

  public static final int DONE_FIELD_NUMBER = 6;
  private boolean done_;
  /**
   * <pre>
   * true if this is the last chunk
   * </pre>
   *
   * <code>optional bool done = 6;</code>
   */
  public boolean getDone() {
    return done_;
  }

  private byte memoizedIsInitialized = -1;
  public final boolean isInitialized() {
    byte isInitialized = memoizedIsInitialized;
    if (isInitialized == 1) return true;
    if (isInitialized == 0) return false;

    memoizedIsInitialized = 1;
    return true;
  }

  public void writeTo(com.google.protobuf.CodedOutputStream output)
                      throws java.io.IOException {
    if (term_ != 0) {
      output.writeInt32(1, term_);
    }
    if (leaderId_ != 0) {
      output.writeInt32(2, leaderId_);
    }
    if (lastIncludedIndex_ != 0L) {
      output.writeInt64(3, lastIncludedIndex_);
    }
    if (lastIncludedTerm_ != 0L) {
      output.writeInt64(4, lastIncludedTerm_);
    }
    if (!data_.isEmpty()) {
      output.writeBytes(5, data_);
    }
    if (done_ != false) {
      output.writeBool(6, done_);
    }
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (term_ != 0) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt32Size(1, term_);
    }
    if (leaderId_ != 0) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt32Size(2, leaderId_);
    }
    if (lastIncludedIndex_ != 0L) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt64Size(3, lastIncludedIndex_);
    }
    if (lastIncludedTerm_ != 0L) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt64Size(4, lastIncludedTerm_);
    }
    if (!data_.isEmpty()) {
      size += com.google.protobuf.CodedOutputStream
        .computeBytesSize(5, data_);
    }
    if (done_ != false) {
      size += com.google.protobuf.CodedOutputStream
        .computeBoolSize(6, done_);
    }
    memoizedSize = size;
    return size;
  }

  private static final long serialVersionUID = 0L;
  @java.lang.Override
  public boolean equals(final java.lang.Object obj) {
    if (obj == this) {
     return true;
    }
    if (!(obj instanceof rsocket.playground.raft.rpc.InstallSnapshotRequest)) {
      return super.equals(obj);
    }
    rsocket.playground.raft.rpc.InstallSnapshotRequest other = (rsocket.playground.raft.rpc.InstallSnapshotRequest) obj;

    boolean result = true;
    result = result && (getTerm()
        == other.getTerm());
    result = result && (getLeaderId()
        == other.getLeaderId());
    result = result && (getLastIncludedIndex()
        == other.getLastIncludedIndex());
    result = result && (getLastIncludedTerm()
        == other.getLastIncludedTerm());
    result = result && getData()
        .equals(other.getData());
    result = result && (getDone()
        == other.getDone());
    return result;
  }

  @java.lang.Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getDescriptorForType().hashCode();
    hash = (37 * hash) + TERM_FIELD_NUMBER;
    hash = (53 * hash) + getTerm();
    hash = (37 * hash) + LEADERID_FIELD_NUMBER;
    hash = (53 * hash) + getLeaderId();
    hash = (37 * hash) + LASTINCLUDEDINDEX_FIELD_NUMBER;
    hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
        getLastIncludedIndex());
    hash = (37 * hash) + LASTINCLUDEDTERM_FIELD_NUMBER;
    hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
        getLastIncludedTerm());
    hash = (37 * hash) + DATA_FIELD_NUMBER;
    hash = (53 * hash) + getData().hashCode();
    hash = (37 * hash) + DONE_FIELD_NUMBER;
    hash = (53 * hash) + com.google.protobuf.Internal.hashBoolean(
        getDone());
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static rsocket.playground.raft.rpc.InstallSnapshotRequest parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static rsocket.playground.raft.rpc.InstallSnapshotRequest parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static rsocket.playground.raft.rpc.InstallSnapshotRequest parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static rsocket.playground.raft.rpc.InstallSnapshotRequest parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static rsocket.playground.raft.rpc.InstallSnapshotRequest parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static rsocket.playground.raft.rpc.InstallSnapshotRequest parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static rsocket.playground.raft.rpc.InstallSnapshotRequest parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static rsocket.playground.raft.rpc.InstallSnapshotRequest parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static rsocket.playground.raft.rpc.InstallSnapshotRequest parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static rsocket.playground.raft.rpc.InstallSnapshotRequest parseFrom(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }

  public Builder newBuilderForType() { return newBuilder(); }
  public static Builder newBuilder() {
    return DEFAULT_INSTANCE.toBuilder();
  }
  public static Builder newBuilder(rsocket.playground.raft.rpc.InstallSnapshotRequest prototype) {
    return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
  }
  public Builder toBuilder() {
    return this == DEFAULT_INSTANCE
        ? new Builder() : new Builder().mergeFrom(this);
  }

  @java.lang.Override
  protected Builder newBuilderForType(
      com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
    Builder builder = new Builder(parent);
    return builder;
  }
  /**
   * Protobuf type {@code InstallSnapshotRequest}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:InstallSnapshotRequest)
      rsocket.playground.raft.rpc.InstallSnapshotRequestOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return rsocket.playground.raft.rpc.Rpc.internal_static_InstallSnapshotRequest_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return rsocket.playground.raft.rpc.Rpc.internal_static_InstallSnapshotRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              rsocket.playground.raft.rpc.InstallSnapshotRequest.class, rsocket.playground.raft.rpc.InstallSnapshotRequest.Builder.class);
    }

    // Construct using rsocket.playground.raft.rpc.InstallSnapshotRequest.newBuilder()
    private Builder() {
      maybeForceBuilderInitialization();
    }

    private Builder(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      super(parent);
      maybeForceBuilderInitialization();
    }
    private void maybeForceBuilderInitialization() {
      if (com.google.protobuf.GeneratedMessageV3
              .alwaysUseFieldBuilders) {
      }
    }
    public Builder clear() {
      super.clear();
      term_ = 0;

      leaderId_ = 0;

      lastIncludedIndex_ = 0L;

      lastIncludedTerm_ = 0L;

      data_ = com.google.protobuf.ByteString.EMPTY;

      done_ = false;

      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return rsocket.playground.raft.rpc.Rpc.internal_static_InstallSnapshotRequest_descriptor;
    }

    public rsocket.playground.raft.rpc.InstallSnapshotRequest getDefaultInstanceForType() {
      return rsocket.playground.raft.rpc.InstallSnapshotRequest.getDefaultInstance();
    }

    public rsocket.playground.raft.rpc.InstallSnapshotRequest build() {
      rsocket.playground.raft.rpc.InstallSnapshotRequest result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public rsocket.playground.raft.rpc.InstallSnapshotRequest buildPartial() {
      rsocket.playground.raft.rpc.InstallSnapshotRequest result = new rsocket.playground.raft.rpc.InstallSnapshotRequest(this);
      result.term_ = term_;
      result.leaderId_ = leaderId_;
      result.lastIncludedIndex_ = lastIncludedIndex_;
      result.lastIncludedTerm_ = lastIncludedTerm_;
      result.data_ = data_;
      result.done_ = done_;
      onBuilt();
      return result;
    }

    public Builder clone() {
      return (Builder) super.clone();
    }
    public Builder setField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        Object value) {
      return (Builder) super.setField(field, value);
    }
    public Builder clearField(
        com.google.protobuf.Descriptors.FieldDescriptor field) {
      return (Builder) super.clearField(field);
    }
    public Builder clearOneof(
        com.google.protobuf.Descriptors.OneofDescriptor oneof) {
      return (Builder) super.clearOneof(oneof);
    }
    public Builder setRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        int index, Object value) {
      return (Builder) super.setRepeatedField(field, index, value);
    }
    public Builder addRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        Object value) {
      return (Builder) super.addRepeatedField(field, value);
    }
    public Builder mergeFrom(com.google.protobuf.Message other) {
      if (other instanceof rsocket.playground.raft.rpc.InstallSnapshotRequest) {
        return mergeFrom((rsocket.playground.raft.rpc.InstallSnapshotRequest)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(rsocket.playground.raft.rpc.InstallSnapshotRequest other) {
      if (other == rsocket.playground.raft.rpc.InstallSnapshotRequest.getDefaultInstance()) return this;
      if (other.getTerm() != 0) {
        setTerm(other.getTerm());
      }
      if (other.getLeaderId() != 0) {
        setLeaderId(other.getLeaderId());
      }
      if (other.getLastIncludedIndex() != 0L) {
        setLastIncludedIndex(other.getLastIncludedIndex());
      }
      if (other.getLastIncludedTerm() != 0L) {
        setLastIncludedTerm(other.getLastIncludedTerm());
      }
      if (other.getData() != com.google.protobuf.ByteString.EMPTY) {
        setData(other.getData());
      }
      if (other.getDone() != false) {
        setDone(other.getDone());
      }
      onChanged();
      return this;
    }

    public final boolean isInitialized() {
      return true;
    }

    public Builder mergeFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      rsocket.playground.raft.rpc.InstallSnapshotRequest parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (rsocket.playground.raft.rpc.InstallSnapshotRequest) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }

    private int term_ ;
    /**
     * <pre>
     * leader’s term
     * </pre>
     *
     * <code>optional int32 term = 1;</code>
     */
    public int getTerm() {
      return term_;
    }
    /**
     * <pre>
     * leader’s term
     * </pre>
     *
     * <code>optional int32 term = 1;</code>
     */
    public Builder setTerm(int value) {
      
      term_ = value;
      onChanged();
      return this;
    }
    /**
     * <pre>
     * leader’s term
     * </pre>
     *
     * <code>optional int32 term = 1;</code>
     */
    public Builder clearTerm() {
      
      term_ = 0;
      onChanged();
      return this;
    }

    private int leaderId_ ;
    /**
     * <pre>
     * so follower can redirect clients
     * </pre>
     *
     * <code>optional int32 leaderId = 2;</code>
     */
    public int getLeaderId() {
      return leaderId_;
    }
    /**
     * <pre>
     * so follower can redirect clients
     * </pre>
     *
     * <code>optional int32 leaderId = 2;</code>
     */
    public Builder setLeaderId(int value) {
      
      leaderId_ = value;
      onChanged();
      return this;
    }
    /**
     * <pre>
     * so follower can redirect clients
     * </pre>
     *
     * <code>optional int32 leaderId = 2;</code>
     */
    public Builder clearLeaderId() {
      
      leaderId_ = 0;
      onChanged();
      return this;
    }

    private long lastIncludedIndex_ ;
    /**
     * <pre>
     * the snapshot replaces all entries up through and including this index
     * </pre>
     *
     * <code>optional int64 lastIncludedIndex = 3;</code>
     */
    public long getLastIncludedIndex() {
      return lastIncludedIndex_;
    }
    /**
     * <pre>
     * the snapshot replaces all entries up through and including this index
     * </pre>
     *
     * <code>optional int64 lastIncludedIndex = 3;</code>
     */
    public Builder setLastIncludedIndex(long value) {
      
      lastIncludedIndex_ = value;
      onChanged();
      return this;
    }
    /**
     * <pre>
     * the snapshot replaces all entries up through and including this index
     * </pre>
     *
     * <code>optional int64 lastIncludedIndex = 3;</code>
     */
    public Builder clearLastIncludedIndex() {
      
      lastIncludedIndex_ = 0L;
      onChanged();
      return this;
    }

    private long lastIncludedTerm_ ;
    /**
     * <pre>
     * lastIncludedTerm term of lastIncludedIndex offset byte offset where chunk is positioned in the snapshot file
     * </pre>
     *
     * <code>optional int64 lastIncludedTerm = 4;</code>
     */
    public long getLastIncludedTerm() {
      return lastIncludedTerm_;
    }
    /**
     * <pre>
     * lastIncludedTerm term of lastIncludedIndex offset byte offset where chunk is positioned in the snapshot file
     * </pre>
     *
     * <code>optional int64 lastIncludedTerm = 4;</code>
     */
    public Builder setLastIncludedTerm(long value) {
      
      lastIncludedTerm_ = value;
      onChanged();
      return this;
    }
    /**
     * <pre>
     * lastIncludedTerm term of lastIncludedIndex offset byte offset where chunk is positioned in the snapshot file
     * </pre>
     *
     * <code>optional int64 lastIncludedTerm = 4;</code>
     */
    public Builder clearLastIncludedTerm() {
      
      lastIncludedTerm_ = 0L;
      onChanged();
      return this;
    }

    private com.google.protobuf.ByteString data_ = com.google.protobuf.ByteString.EMPTY;
    /**
     * <pre>
     * raw bytes of the snapshot chunk, starting at offset
     * </pre>
     *
     * <code>optional bytes data = 5;</code>
     */
    public com.google.protobuf.ByteString getData() {
      return data_;
    }
    /**
     * <pre>
     * raw bytes of the snapshot chunk, starting at offset
     * </pre>
     *
     * <code>optional bytes data = 5;</code>
     */
    public Builder setData(com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  
      data_ = value;
      onChanged();
      return this;
    }
    /**
     * <pre>
     * raw bytes of the snapshot chunk, starting at offset
     * </pre>
     *
     * <code>optional bytes data = 5;</code>
     */
    public Builder clearData() {
      
      data_ = getDefaultInstance().getData();
      onChanged();
      return this;
    }

    private boolean done_ ;
    /**
     * <pre>
     * true if this is the last chunk
     * </pre>
     *
     * <code>optional bool done = 6;</code>
     */
    public boolean getDone() {
      return done_;
    }
    /**
     * <pre>
     * true if this is the last chunk
     * </pre>
     *
     * <code>optional bool done = 6;</code>
     */
    public Builder setDone(boolean value) {
      
      done_ = value;
      onChanged();
      return this;
    }
    /**
     * <pre>
     * true if this is the last chunk
     * </pre>
     *
     * <code>optional bool done = 6;</code>
     */
    public Builder clearDone() {
      
      done_ = false;
      onChanged();
      return this;
    }
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return this;
    }

    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return this;
    }


    // @@protoc_insertion_point(builder_scope:InstallSnapshotRequest)
  }

  // @@protoc_insertion_point(class_scope:InstallSnapshotRequest)
  private static final rsocket.playground.raft.rpc.InstallSnapshotRequest DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new rsocket.playground.raft.rpc.InstallSnapshotRequest();
  }

  public static rsocket.playground.raft.rpc.InstallSnapshotRequest getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<InstallSnapshotRequest>
      PARSER = new com.google.protobuf.AbstractParser<InstallSnapshotRequest>() {
    public InstallSnapshotRequest parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
        return new InstallSnapshotRequest(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<InstallSnapshotRequest> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<InstallSnapshotRequest> getParserForType() {
    return PARSER;
  }

  public rsocket.playground.raft.rpc.InstallSnapshotRequest getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

