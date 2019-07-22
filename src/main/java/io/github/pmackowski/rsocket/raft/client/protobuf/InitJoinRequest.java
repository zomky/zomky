// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: client.proto

package io.github.pmackowski.rsocket.raft.client.protobuf;

/**
 * Protobuf type {@code InitJoinRequest}
 */
public  final class InitJoinRequest extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:InitJoinRequest)
    InitJoinRequestOrBuilder {
  // Use InitJoinRequest.newBuilder() to construct.
  private InitJoinRequest(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private InitJoinRequest() {
    requesterPort_ = 0;
    host_ = "";
    port_ = 0;
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return com.google.protobuf.UnknownFieldSet.getDefaultInstance();
  }
  private InitJoinRequest(
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

            requesterPort_ = input.readInt32();
            break;
          }
          case 18: {
            java.lang.String s = input.readStringRequireUtf8();

            host_ = s;
            break;
          }
          case 24: {

            port_ = input.readInt32();
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
    return io.github.pmackowski.rsocket.raft.client.protobuf.Client.internal_static_InitJoinRequest_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return io.github.pmackowski.rsocket.raft.client.protobuf.Client.internal_static_InitJoinRequest_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest.class, io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest.Builder.class);
  }

  public static final int REQUESTER_PORT_FIELD_NUMBER = 1;
  private int requesterPort_;
  /**
   * <code>optional int32 requester_port = 1;</code>
   */
  public int getRequesterPort() {
    return requesterPort_;
  }

  public static final int HOST_FIELD_NUMBER = 2;
  private volatile java.lang.Object host_;
  /**
   * <code>optional string host = 2;</code>
   */
  public java.lang.String getHost() {
    java.lang.Object ref = host_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      host_ = s;
      return s;
    }
  }
  /**
   * <code>optional string host = 2;</code>
   */
  public com.google.protobuf.ByteString
      getHostBytes() {
    java.lang.Object ref = host_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      host_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int PORT_FIELD_NUMBER = 3;
  private int port_;
  /**
   * <code>optional int32 port = 3;</code>
   */
  public int getPort() {
    return port_;
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
    if (requesterPort_ != 0) {
      output.writeInt32(1, requesterPort_);
    }
    if (!getHostBytes().isEmpty()) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 2, host_);
    }
    if (port_ != 0) {
      output.writeInt32(3, port_);
    }
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (requesterPort_ != 0) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt32Size(1, requesterPort_);
    }
    if (!getHostBytes().isEmpty()) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, host_);
    }
    if (port_ != 0) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt32Size(3, port_);
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
    if (!(obj instanceof io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest)) {
      return super.equals(obj);
    }
    io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest other = (io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest) obj;

    boolean result = true;
    result = result && (getRequesterPort()
        == other.getRequesterPort());
    result = result && getHost()
        .equals(other.getHost());
    result = result && (getPort()
        == other.getPort());
    return result;
  }

  @java.lang.Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getDescriptorForType().hashCode();
    hash = (37 * hash) + REQUESTER_PORT_FIELD_NUMBER;
    hash = (53 * hash) + getRequesterPort();
    hash = (37 * hash) + HOST_FIELD_NUMBER;
    hash = (53 * hash) + getHost().hashCode();
    hash = (37 * hash) + PORT_FIELD_NUMBER;
    hash = (53 * hash) + getPort();
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest parseFrom(
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
  public static Builder newBuilder(io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest prototype) {
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
   * Protobuf type {@code InitJoinRequest}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:InitJoinRequest)
      io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequestOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return io.github.pmackowski.rsocket.raft.client.protobuf.Client.internal_static_InitJoinRequest_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return io.github.pmackowski.rsocket.raft.client.protobuf.Client.internal_static_InitJoinRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest.class, io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest.Builder.class);
    }

    // Construct using io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest.newBuilder()
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
      requesterPort_ = 0;

      host_ = "";

      port_ = 0;

      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return io.github.pmackowski.rsocket.raft.client.protobuf.Client.internal_static_InitJoinRequest_descriptor;
    }

    public io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest getDefaultInstanceForType() {
      return io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest.getDefaultInstance();
    }

    public io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest build() {
      io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest buildPartial() {
      io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest result = new io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest(this);
      result.requesterPort_ = requesterPort_;
      result.host_ = host_;
      result.port_ = port_;
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
      if (other instanceof io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest) {
        return mergeFrom((io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest other) {
      if (other == io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest.getDefaultInstance()) return this;
      if (other.getRequesterPort() != 0) {
        setRequesterPort(other.getRequesterPort());
      }
      if (!other.getHost().isEmpty()) {
        host_ = other.host_;
        onChanged();
      }
      if (other.getPort() != 0) {
        setPort(other.getPort());
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
      io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }

    private int requesterPort_ ;
    /**
     * <code>optional int32 requester_port = 1;</code>
     */
    public int getRequesterPort() {
      return requesterPort_;
    }
    /**
     * <code>optional int32 requester_port = 1;</code>
     */
    public Builder setRequesterPort(int value) {
      
      requesterPort_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional int32 requester_port = 1;</code>
     */
    public Builder clearRequesterPort() {
      
      requesterPort_ = 0;
      onChanged();
      return this;
    }

    private java.lang.Object host_ = "";
    /**
     * <code>optional string host = 2;</code>
     */
    public java.lang.String getHost() {
      java.lang.Object ref = host_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        host_ = s;
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string host = 2;</code>
     */
    public com.google.protobuf.ByteString
        getHostBytes() {
      java.lang.Object ref = host_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        host_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string host = 2;</code>
     */
    public Builder setHost(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  
      host_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional string host = 2;</code>
     */
    public Builder clearHost() {
      
      host_ = getDefaultInstance().getHost();
      onChanged();
      return this;
    }
    /**
     * <code>optional string host = 2;</code>
     */
    public Builder setHostBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
      
      host_ = value;
      onChanged();
      return this;
    }

    private int port_ ;
    /**
     * <code>optional int32 port = 3;</code>
     */
    public int getPort() {
      return port_;
    }
    /**
     * <code>optional int32 port = 3;</code>
     */
    public Builder setPort(int value) {
      
      port_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional int32 port = 3;</code>
     */
    public Builder clearPort() {
      
      port_ = 0;
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


    // @@protoc_insertion_point(builder_scope:InitJoinRequest)
  }

  // @@protoc_insertion_point(class_scope:InitJoinRequest)
  private static final io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest();
  }

  public static io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<InitJoinRequest>
      PARSER = new com.google.protobuf.AbstractParser<InitJoinRequest>() {
    public InitJoinRequest parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
        return new InitJoinRequest(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<InitJoinRequest> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<InitJoinRequest> getParserForType() {
    return PARSER;
  }

  public io.github.pmackowski.rsocket.raft.client.protobuf.InitJoinRequest getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}
