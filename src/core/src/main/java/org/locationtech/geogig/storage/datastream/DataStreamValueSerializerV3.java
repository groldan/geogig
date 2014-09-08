/*******************************************************************************
 * Copyright (c) 2014 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *******************************************************************************/
package org.locationtech.geogig.storage.datastream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;

import org.locationtech.geogig.repository.Hints;
import org.locationtech.geogig.storage.FieldType;

import com.google.common.base.Optional;

/**
 * A class to serializer/deserialize attribute values to/from a data stream
 * 
 */
class DataStreamValueSerializerV3 {

    static final Map<FieldType, ValueSerializer> serializers = new EnumMap<>(
            DataStreamValueSerializerV2.serializers);
    static {
        ValueSerializer geometry = GeometrySerializer.defaultInstance();
        serializers.put(FieldType.GEOMETRY, geometry);
        serializers.put(FieldType.POINT, geometry);
        serializers.put(FieldType.LINESTRING, geometry);
        serializers.put(FieldType.POLYGON, geometry);
        serializers.put(FieldType.MULTIPOINT, geometry);
        serializers.put(FieldType.MULTILINESTRING, geometry);
        serializers.put(FieldType.MULTIPOLYGON, geometry);
        serializers.put(FieldType.GEOMETRYCOLLECTION, geometry);
    }

    /**
     * Writes the passed attribute value in the specified data stream
     */
    public static void write(Optional<Object> opt, DataOutput data) throws IOException {
        FieldType type = FieldType.forValue(opt);
        if (serializers.containsKey(type)) {
            serializers.get(type).write(opt.orNull(), data);
        } else {
            throw new IllegalArgumentException("The specified type (" + type + ") is not supported");
        }
    }

    /**
     * Reads an object of the specified type from the provided data stream
     */
    public static Object read(FieldType type, DataInput in) throws IOException {
        return read(type, in, Hints.nil());
    }

    public static Object read(FieldType type, DataInput in, Hints hints) throws IOException {
        if (serializers.containsKey(type)) {
            return serializers.get(type).read(in, hints);
        }
        throw new IllegalArgumentException("The specified type is not supported");
    }
}
