package io.confluent.ps.examples;

import org.apache.kafka.common.header.Header;

import java.nio.ByteBuffer;

public class LatencyHeader implements Header {

  public static final String HEADER_KEY = "origination-time-ms";

  private long timestamp;

  public LatencyHeader(long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public String key() {
    return HEADER_KEY;
  }

  @Override
  public byte[] value() {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(timestamp);
    return buffer.array();
  }

  public long valueTimestamp() {
    return this.timestamp;
  }

  public static long byteArrayToLong(byte[] value) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.put(value);
    buffer.flip();
    return buffer.getLong();
  }
}
