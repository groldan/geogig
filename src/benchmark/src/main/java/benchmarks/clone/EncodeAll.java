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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import org.locationtech.geogig.model.RevObject;
import org.locationtech.geogig.storage.datastream.DataStreamSerializationFactoryV1;
import org.locationtech.geogig.storage.datastream.DataStreamSerializationFactoryV2;
import org.locationtech.geogig.storage.datastream.DataStreamSerializationFactoryV2_1;
import org.locationtech.geogig.storage.datastream.DataStreamSerializationFactoryV2_2;
import org.locationtech.geogig.storage.datastream.LZ4SerializationFactory;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CountingOutputStream;

import benchmarks.states.AllObjectsState;
import benchmarks.states.ExternalResources;
import benchmarks.states.RepoState;
import net.jpountz.lz4.LZ4BlockOutputStream;

/**
 * Benchmarks the optimal times for encoding and decoding all revision objects in the repository
 *
 */
@State(Scope.Benchmark)
@BenchmarkMode({ Mode.AverageTime })
@Warmup(iterations = 0)
@Measurement(iterations = 1)
@Threads(1)
@Fork(1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class EncodeAll {

    private static final Map<String, ObjectSerializingFactory> SERIALIZERS = //
            ImmutableMap.of(//
                    "V1_0", DataStreamSerializationFactoryV1.INSTANCE//
                    , "V2_0", DataStreamSerializationFactoryV2.INSTANCE//
                    , "V2_1", DataStreamSerializationFactoryV2_1.INSTANCE//
                    , "V2_2", DataStreamSerializationFactoryV2_2.INSTANCE//
                    , "V2_3", DataStreamSerializationFactoryV2_3.INSTANCE//
            );

    /**
     * State for reporting auxiliary statistics
     */
    @State(Scope.Thread)
    @AuxCounters(Type.EVENTS)
    public static class Counter {
        public long count;

        public long totalBytes;

        public @Setup void setup() {
            this.count = 0;
            this.totalBytes = 0;
        }
    }

    @Param(value = { /* "V1_0", "V2_0", "V2_1", "V2_2", */ "V2_3" })
    public String serializer;

    @Param(value = { //
            "None", //
            "GZIP_Stream", //
            "LZ4_Stream", //
            "LZ4_Per_Object"//
    })
    public String compression;

    private ObjectSerializingFactory serializationFactory;

    private Path targetFile;

    private OutputStream outputStream;

    public @Setup void setup(ExternalResources resources, RepoState repo) throws IOException {
        Path tempdirectory = resources.getTempdirectory();

        String prefix = repo.repoName + "_" + compression;
        targetFile = Files.createTempFile(tempdirectory, prefix, ".geogig");

        ObjectSerializingFactory factory = SERIALIZERS.get(serializer);
        Preconditions.checkArgument(factory != null, serializer + " not configured");
        this.serializationFactory = factory;
    }

    public @TearDown void tearDown() throws IOException {
        try {
            outputStream.close();
        } catch (Exception ignore) {

        } finally {
            Files.delete(targetFile);
        }
    }

    @Benchmark
    public void trees(Blackhole bh, AllObjectsState state, Counter counter) throws IOException {
        Iterable<? extends RevObject> objects = state.getAllTrees();
        encode(bh, objects, counter);
    }

    @Benchmark
    public void buckets(Blackhole bh, AllObjectsState state, Counter counter) throws IOException {
        Iterable<? extends RevObject> objects = state.getAllBuckets();
        encode(bh, objects, counter);
    }

    @Benchmark
    public void features(Blackhole bh, AllObjectsState state, Counter counter) throws IOException {
        Iterable<? extends RevObject> objects = state.getAllFeatures();
        encode(bh, objects, counter);
    }

    @Benchmark
    public void commits(Blackhole bh, AllObjectsState state, Counter counter) throws IOException {
        Iterable<? extends RevObject> objects = state.getAllCommits();
        encode(bh, objects, counter);
    }

    @Benchmark
    public void featureTypes(Blackhole bh, AllObjectsState state, Counter counter)
            throws IOException {
        Iterable<? extends RevObject> objects = state.getAllFeatureTypes();
        encode(bh, objects, counter);
    }

    private void encode(Blackhole bh, Iterable<? extends RevObject> objects, Counter counter)
            throws IOException {

        counter.count = 0;
        counter.totalBytes = 0;

        outputStream = Files.newOutputStream(targetFile);
        CountingOutputStream countingStream = new CountingOutputStream(outputStream);
        OutputStream target = countingStream;
        if ("GZIP_Stream".equals(compression)) {
            target = new GZIPOutputStream(target);
        } else if ("LZ4_Stream".equals(compression)) {
            target = new LZ4BlockOutputStream(target);
        } else if ("LZ4_Per_Object".equals(compression)) {
            this.serializationFactory = new LZ4SerializationFactory(serializationFactory);
        }
        Stopwatch sw = Stopwatch.createStarted();
        for (RevObject obj : objects) {
            serializationFactory.write(obj, target);
            counter.count++;
            bh.consume(obj);
        }
        target.flush();
        target.close();
        sw.stop();
        long count = countingStream.getCount();
        counter.totalBytes = count;
    }

}
