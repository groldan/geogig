/*******************************************************************************
 * Copyright (c) 2014 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *******************************************************************************/
package org.locationtech.geogig.storage.datastream;

import static org.locationtech.geogig.storage.datastream.FormatCommonV2.readHeader;
import static org.locationtech.geogig.storage.datastream.Varint.readUnsignedVarInt;
import static org.locationtech.geogig.storage.datastream.Varint.writeUnsignedVarInt;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.EnumMap;
import java.util.Map;

import org.locationtech.geogig.api.ObjectId;
import org.locationtech.geogig.api.RevCommit;
import org.locationtech.geogig.api.RevFeature;
import org.locationtech.geogig.api.RevFeatureImpl;
import org.locationtech.geogig.api.RevFeatureType;
import org.locationtech.geogig.api.RevObject;
import org.locationtech.geogig.api.RevObject.TYPE;
import org.locationtech.geogig.api.RevTree;
import org.locationtech.geogig.storage.FieldType;
import org.locationtech.geogig.storage.ObjectReader;
import org.locationtech.geogig.storage.ObjectSerializingFactory;
import org.locationtech.geogig.storage.ObjectWriter;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

/**
 * Serialization factory for serial version 2
 */
public class DataStreamSerializationFactoryV3 implements ObjectSerializingFactory {

    public static final DataStreamSerializationFactoryV3 INSTANCE = new DataStreamSerializationFactoryV3();

    private final static ObjectReader<RevObject> OBJECT_READER = new ObjectReaderV3();

    private static final EnumMap<TYPE, Serializer<? extends RevObject>> serializers = new EnumMap<>(
            DataStreamSerializationFactoryV2.serializers);
    static {
        serializers.put(TYPE.FEATURE, new FeatureSerializer());
    }

    @SuppressWarnings("unchecked")
    private static <T extends RevObject> Serializer<T> serializer(TYPE type) {
        Serializer<? extends RevObject> serializer = serializers.get(type);
        if (serializer == null) {
            throw new UnsupportedOperationException("No serializer for " + type);
        }
        return (Serializer<T>) serializer;
    }

    @Override
    public ObjectReader<RevCommit> createCommitReader() {
        return serializer(TYPE.COMMIT);
    }

    @Override
    public ObjectReader<RevTree> createRevTreeReader() {
        return serializer(TYPE.TREE);
    }

    @Override
    public ObjectReader<RevFeature> createFeatureReader() {
        return serializer(TYPE.FEATURE);
    }

    @Override
    public ObjectReader<RevFeature> createFeatureReader(Map<String, Serializable> hints) {
        return serializer(TYPE.FEATURE);
    }

    @Override
    public ObjectReader<RevFeatureType> createFeatureTypeReader() {
        return serializer(TYPE.FEATURETYPE);
    }

    @Override
    public <T extends RevObject> ObjectWriter<T> createObjectWriter(TYPE type) {
        return serializer(type);
    }

    @Override
    public <T extends RevObject> ObjectReader<T> createObjectReader(TYPE type) {
        return serializer(type);
    }

    @Override
    public ObjectReader<RevObject> createObjectReader() {
        return OBJECT_READER;
    }

    private static final class FeatureSerializer extends Serializer<RevFeature> {

        FeatureSerializer() {
            super(TYPE.FEATURE);
        }

        @Override
        public RevFeature readBody(ObjectId id, DataInput in) throws IOException {
            return readFeature(id, in);
        }

        @Override
        public void writeBody(RevFeature feature, DataOutput data) throws IOException {
            writeFeature(feature, data);
        }
    }

    public static void writeFeature(RevFeature feature, DataOutput data) throws IOException {
        ImmutableList<Optional<Object>> values = feature.getValues();

        writeUnsignedVarInt(values.size(), data);

        for (Optional<Object> field : values) {
            FieldType type = FieldType.forValue(field);
            data.writeByte(type.getTag());
            if (type != FieldType.NULL) {
                DataStreamValueSerializerV3.write(field, data);
            }
        }
    }

    public static RevFeature readFeature(ObjectId id, DataInput in) throws IOException {
        final int count = readUnsignedVarInt(in);
        final ImmutableList.Builder<Optional<Object>> builder = ImmutableList.builder();

        for (int i = 0; i < count; i++) {
            final byte fieldTag = in.readByte();
            final FieldType fieldType = FieldType.valueOf(fieldTag);
            Object value = DataStreamValueSerializerV3.read(fieldType, in);
            builder.add(Optional.fromNullable(value));
        }

        return new RevFeatureImpl(id, builder.build());
    }

    private static final class ObjectReaderV3 implements ObjectReader<RevObject> {
        @Override
        public RevObject read(ObjectId id, InputStream rawData) throws IllegalArgumentException {
            DataInput in = new DataInputStream(rawData);
            try {
                return readData(id, in);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private RevObject readData(ObjectId id, DataInput in) throws IOException {
            final TYPE type = readHeader(in);
            Serializer<RevObject> serializer = DataStreamSerializationFactoryV3.serializer(type);
            RevObject object = serializer.readBody(id, in);
            return object;
        }
    }
}
