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

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.locationtech.geogig.api.ObjectId;
import org.locationtech.geogig.api.RevCommit;
import org.locationtech.geogig.api.RevFeature;
import org.locationtech.geogig.api.RevFeatureType;
import org.locationtech.geogig.api.RevObject;
import org.locationtech.geogig.api.RevObject.TYPE;
import org.locationtech.geogig.api.RevTree;
import org.locationtech.geogig.repository.Hints;
import org.locationtech.geogig.storage.FieldType;
import org.locationtech.geogig.storage.ObjectReader;
import org.locationtech.geogig.storage.ObjectSerializingFactory;
import org.locationtech.geogig.storage.ObjectWriter;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;

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

    @SuppressWarnings("unchecked")
    @Override
    public <T extends RevObject> ObjectReader<T> createObjectReader(@Nullable TYPE type) {
        if (type == null) {
            return (ObjectReader<T>) OBJECT_READER;
        }
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
        public RevFeature readBody(ObjectId id, DataInput in, Hints hints) throws IOException {
            return readFeature(id, in, hints);
        }

        @Override
        public void writeBody(RevFeature feature, DataOutput data) throws IOException {
            writeFeature(feature, data);
        }
    }

    public static void writeFeature(RevFeature feature, DataOutput data) throws IOException {
        ImmutableList<Optional<Object>> values = feature.getValues();

        final int attCount = values.size();

        ByteArrayOutputStream header = new ByteArrayOutputStream(attCount + 10);
        ByteArrayOutputStream dataBuff = new ByteArrayOutputStream();
        {
            DataOutputStream headerOut = new DataOutputStream(header);
            DataOutputStream dataOut = new DataOutputStream(dataBuff);

            writeUnsignedVarInt(attCount, headerOut);

            for (int i = 0; i < attCount; i++) {
                Optional<Object> field = values.get(i);
                FieldType type = FieldType.forValue(field);
                int offset = dataOut.size();
                dataOut.writeByte(type.getTag());
                if (type != FieldType.NULL) {
                    DataStreamValueSerializerV3.write(field, dataOut);
                }

                writeUnsignedVarInt(offset, headerOut);
            }
            headerOut.flush();
            dataOut.flush();
        }

        data.write(header.toByteArray());
        data.write(dataBuff.toByteArray());
    }

    public static RevFeature readFeature(ObjectId id, DataInput in, Hints hints) throws IOException {
        try {
            final int count = readUnsignedVarInt(in);
            List<Integer> offsets = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                offsets.add(Integer.valueOf(readUnsignedVarInt(in)));
            }

            final byte[] dataArray = ByteStreams.toByteArray((InputStream) in);
            final AttributeReader reader = new AttributeReader(dataArray, hints);

            List<Optional<Object>> lazyList = Lists.transform(offsets, reader);

            return new LazyRevFeature(id, lazyList);
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw e;
        }
    }

    private static class AttributeReader implements Function<Integer, Optional<Object>> {
        private final byte[] buff;

        private Hints hints;

        public AttributeReader(byte[] data, Hints hints) {
            this.buff = data;
            this.hints = hints;
        }

        @Override
        public Optional<Object> apply(Integer offset) {
            Object object;
            try {
                DataInput in = ByteStreams.newDataInput(buff, offset.intValue());
                final byte fieldTag = in.readByte();
                final FieldType fieldType = FieldType.valueOf(fieldTag);
                object = DataStreamValueSerializerV3.read(fieldType, in, hints);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
            return Optional.fromNullable(object);
        }
    };

    private static final class ObjectReaderV3 implements ObjectReader<RevObject> {

        @Override
        public RevObject read(ObjectId id, InputStream rawData) throws IllegalArgumentException {
            return read(id, rawData, Hints.nil());
        }

        @Override
        public RevObject read(ObjectId id, InputStream rawData, Hints hints) {
            DataInput in = new DataInputStream(rawData);
            try {
                return readData(id, in, hints);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private RevObject readData(ObjectId id, DataInput in, Hints hints) throws IOException {
            final TYPE type = readHeader(in);
            Serializer<RevObject> serializer = DataStreamSerializationFactoryV3.serializer(type);
            RevObject object = serializer.readBody(id, in, hints);
            return object;
        }
    }
}
