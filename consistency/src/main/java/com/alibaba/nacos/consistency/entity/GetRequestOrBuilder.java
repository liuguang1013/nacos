// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: Data.proto

package com.alibaba.nacos.consistency.entity;

public interface GetRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:GetRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>string group = 1;</code>
   * @return The group.
   */
  String getGroup();
  /**
   * <code>string group = 1;</code>
   * @return The bytes for group.
   */
  com.google.protobuf.ByteString
      getGroupBytes();

  /**
   * <code>bytes data = 2;</code>
   * @return The data.
   */
  com.google.protobuf.ByteString getData();

  /**
   * <code>map&lt;string, string&gt; extendInfo = 3;</code>
   */
  int getExtendInfoCount();
  /**
   * <code>map&lt;string, string&gt; extendInfo = 3;</code>
   */
  boolean containsExtendInfo(
      String key);
  /**
   * Use {@link #getExtendInfoMap()} instead.
   */
  @Deprecated
  java.util.Map<String, String>
  getExtendInfo();
  /**
   * <code>map&lt;string, string&gt; extendInfo = 3;</code>
   */
  java.util.Map<String, String>
  getExtendInfoMap();
  /**
   * <code>map&lt;string, string&gt; extendInfo = 3;</code>
   */
  /* nullable */
String getExtendInfoOrDefault(
      String key,
      /* nullable */
String defaultValue);
  /**
   * <code>map&lt;string, string&gt; extendInfo = 3;</code>
   */
  String getExtendInfoOrThrow(
      String key);
}
