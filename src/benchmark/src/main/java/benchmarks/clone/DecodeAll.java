/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package benchmarks.clone;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.RevObject;
import org.locationtech.geogig.storage.datastream.DataStreamSerializationFactoryV1;
import org.locationtech.geogig.storage.datastream.DataStreamSerializationFactoryV2;
import org.locationtech.geogig.storage.datastream.DataStreamSerializationFactoryV2_1;
import org.locationtech.geogig.storage.datastream.DataStreamSerializationFactoryV2_2;
import org.locationtech.geogig.storage.datastream.v2_3.DataStreamSerializationFactoryV2_3;
import org.locationtech.geogig.storage.impl.ObjectSerializingFactory;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.AuxCounters.Type;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import benchmarks.states.AllObjectsState;

/**
 * Benchmarks the optimal times for encoding and decoding all revision objects in the repository
 *
 */
@State(Scope.Benchmark)
@BenchmarkMode({ Mode.AverageTime })
@Warmup(iterations = 1)
@Measurement(iterations = 1)
@Threads(1)
@Fork(1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class DecodeAll {

    private static final Map<String, ObjectSerializingFactory> SERIALIZERS = //
            ImmutableMap.of(//
                    "V1_0", DataStreamSerializationFactoryV1.INSTANCE//
                    , "V2_0", DataStreamSerializationFactoryV2.INSTANCE//
                    , "V2_1", DataStreamSerializationFactoryV2_1.INSTANCE//
                    , "V2_2", DataStreamSerializationFactoryV2_2.INSTANCE//
                    , "V2_3", DataStreamSerializationFactoryV2_3.INSTANCE//
            );

    @Param(value = { /* "V1_0", "V2_0", "V2_1", "V2_2", */ "V2_3" })
    public String serializer;

    private ObjectSerializingFactory serializationFactory;

    public @Setup void setup() {
        ObjectSerializingFactory factory = SERIALIZERS.get(serializer);
        Preconditions.checkArgument(factory != null, serializer + " not configured");
        this.serializationFactory = factory;
    }

    /**
     * State for reporting auxiliary statistics
     */
    @State(Scope.Thread)
    @AuxCounters(Type.EVENTS)
    public static class Counter {
        public long count;
    }

    private RevObject decode(DataInputStream in) throws IOException {
        ObjectId id = ObjectId.readFrom(in);
        RevObject object = serializationFactory.read(id, in);
        return object;
    }

    private void decode(Blackhole bh, Counter counter, InputStream objectsStream)
            throws IOException {
        counter.count = 0;
        try {
            DataInputStream in = new DataInputStream(objectsStream);
            while (true) {
                RevObject object = decode(in);
                bh.consume(object);
                counter.count++;
            }
        } catch (EOFException expected) {
            // ok
            // counter.count = count;
            // System.err.printf("## Decoded %,d objects.\n", count);
        } finally {
            objectsStream.close();
        }

    }

    @Benchmark
    public void trees(Blackhole bh, AllObjectsState state, Counter counter) throws IOException {
        decode(bh, counter, state.getAllTreesStream());
    }

    @Benchmark
    public void buckets(Blackhole bh, AllObjectsState state, Counter counter) throws IOException {
        decode(bh, counter, state.getAllBucketsStream());
    }

    @Benchmark
    public void features(Blackhole bh, AllObjectsState state, Counter counter) throws IOException {
        decode(bh, counter, state.getAllFeaturesStream());
    }

    @Benchmark
    public void commits(Blackhole bh, AllObjectsState state, Counter counter) throws IOException {
        decode(bh, counter, state.getAllCommitsStream());
    }

    @Benchmark
    public void featureTypes(Blackhole bh, AllObjectsState state, Counter counter)
            throws IOException {
        decode(bh, counter, state.getAllFeatureTypesStream());
    }
}
