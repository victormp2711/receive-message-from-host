package com.gracaconsultores.messages.models;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;

public class BigSerializedObjectCodec implements MessageCodec<BigSerializedObject, BigSerializedObject> {
  @Override
  public void encodeToWire(Buffer buffer, BigSerializedObject o) {
    System.out.println("encodeToWire");
  }

  @Override
  public BigSerializedObject decodeFromWire(int pos, Buffer buffer) {
    System.out.println("decodeFromWire");
    return new BigSerializedObject();
  }

  @Override
  public BigSerializedObject transform(BigSerializedObject o) {
    System.out.println("transform");
    return o;
  }

  @Override
  public String name() {
    return "BrokenSerializedObjectCodec";
  }

  @Override
  public byte systemCodecID() {
    return -1;
  }
}
