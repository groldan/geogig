// automatically generated by the FlatBuffers compiler, do not modify

package org.locationtech.geogig.flatbuffers.generated.values;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.google.flatbuffers.FlatBufferBuilder;
import com.google.flatbuffers.Table;

@SuppressWarnings("unused")
public final class BOOLEAN_ARRAY extends Table {
  public static BOOLEAN_ARRAY getRootAsBOOLEAN_ARRAY(ByteBuffer _bb) { return getRootAsBOOLEAN_ARRAY(_bb, new BOOLEAN_ARRAY()); }
  public static BOOLEAN_ARRAY getRootAsBOOLEAN_ARRAY(ByteBuffer _bb, BOOLEAN_ARRAY obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; }
  public BOOLEAN_ARRAY __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public boolean value(int j) { int o = __offset(4); return o != 0 ? 0!=bb.get(__vector(o) + j * 1) : false; }
  public int valueLength() { int o = __offset(4); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer valueAsByteBuffer() { return __vector_as_bytebuffer(4, 1); }
  public ByteBuffer valueInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 4, 1); }

  public static int createBOOLEAN_ARRAY(FlatBufferBuilder builder,
      int valueOffset) {
    builder.startObject(1);
    BOOLEAN_ARRAY.addValue(builder, valueOffset);
    return BOOLEAN_ARRAY.endBOOLEAN_ARRAY(builder);
  }

  public static void startBOOLEAN_ARRAY(FlatBufferBuilder builder) { builder.startObject(1); }
  public static void addValue(FlatBufferBuilder builder, int valueOffset) { builder.addOffset(0, valueOffset, 0); }
  public static int createValueVector(FlatBufferBuilder builder, boolean[] data) { builder.startVector(1, data.length, 1); for (int i = data.length - 1; i >= 0; i--) builder.addBoolean(data[i]); return builder.endVector(); }
  public static void startValueVector(FlatBufferBuilder builder, int numElems) { builder.startVector(1, numElems, 1); }
  public static int endBOOLEAN_ARRAY(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
}
