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

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.locationtech.geogig.model.RevCommit;
import org.locationtech.geogig.model.RevObject;
import org.locationtech.geogig.repository.Repository;
import org.locationtech.geogig.storage.BulkOpListener;
import org.locationtech.geogig.storage.BulkOpListener.CountingListener;
import org.locationtech.geogig.storage.GraphDatabase;
import org.locationtech.geogig.storage.GraphDatabase.GraphNode;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.locationtech.geogig.storage.impl.ForwardingObjectDatabase;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.AuxCounters.Type;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import com.google.common.base.Preconditions;

import benchmarks.states.AllObjectsState;
import benchmarks.states.EmptyRepositoryState;

/**
 * Benchmarks the optimal times for querying all objects reachable from the repository's revision
 * graph
 *
 */
@State(Scope.Benchmark)
@BenchmarkMode({ Mode.AverageTime })
@Warmup(iterations = 0)
@Measurement(iterations = 1)
@Threads(1)
@Fork(1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class InsertAllObjects {

    /**
     * State for reporting auxiliary statistics
     */
    @State(Scope.Thread)
    @AuxCounters(Type.EVENTS)
    public static class Counter {
        public long inserted, repeated;
    }

    // @Benchmark
    public void commitsNoGraph(EmptyRepositoryState target, AllObjectsState data, Counter counter) {

        Repository targetrepo = target.getRepository();
        ObjectDatabase db = targetrepo.objectDatabase();
        if (db instanceof ForwardingObjectDatabase) {
            db = ((ForwardingObjectDatabase) db).unwrap();
        }

        insertAll(counter, data.getAllCommits(), db);
    }

    // @Benchmark
    public void commitsGraphOnly(EmptyRepositoryState target, AllObjectsState data,
            Counter counter) {

        Repository targetrepo = target.getRepository();

        GraphDatabase graphDatabase = targetrepo.graphDatabase();
        for (RevCommit c : data.getAllCommits()) {
            graphDatabase.put(c.getId(), c.getParentIds());
        }
    }

    // @Benchmark
    public void commitsGraphOptimized(EmptyRepositoryState target, AllObjectsState data,
            Counter counter) {

        counter.inserted = 0;

        Repository targetrepo = target.getRepository();

        GraphDatabase graphDatabase = targetrepo.graphDatabase();
        graphDatabase.putAll(data.getAllCommits());

        for (RevCommit c : data.getAllCommits()) {
            GraphNode node = graphDatabase.getNode(c.getId());
            Preconditions.checkNotNull(node);
            counter.inserted++;
        }
    }

//    @Benchmark
    public void commits(EmptyRepositoryState target, AllObjectsState data, Counter counter) {

        insertAll(target, counter, data.getAllCommits());
    }

    @Benchmark
    public void trees(EmptyRepositoryState target, AllObjectsState data, Counter counter) {

        insertAll(target, counter, data.getAllTrees());
    }

    @Benchmark
    public void buckets(EmptyRepositoryState target, AllObjectsState data, Counter counter) {

        insertAll(target, counter, data.getAllBuckets());
    }

    @Benchmark
    public void features(EmptyRepositoryState target, AllObjectsState data, Counter counter) {

        insertAll(target, counter, data.getAllFeatures());
    }

    @Benchmark
    public void featureTypes(EmptyRepositoryState target, AllObjectsState data, Counter counter) {

        insertAll(target, counter, data.getAllFeatureTypes());
    }

    /**
     * What it takes to insert all the objects from the origin repository onto the target
     * {@link ObjectDatabase} under the ideal conditions that we already have all the objects
     * pre-computed.
     */
    // @Benchmark
    public void insertAllObjects(EmptyRepositoryState target, AllObjectsState data,
            Counter counter) {

        Iterable<RevObject> objects = data.getAllObjects();
        insertAll(target, counter, objects);
    }

    private void insertAll(EmptyRepositoryState target, Counter counter,
            Iterable<? extends RevObject> objects) {
        Repository repo = target.getRepository();
        ObjectDatabase db = repo.objectDatabase();
        insertAll(counter, objects, db);
    }

    private void insertAll(Counter counter, Iterable<? extends RevObject> objects,
            ObjectDatabase db) {
        counter.inserted = 0;
        counter.repeated = 0;
        CountingListener listener = BulkOpListener.newCountingListener();
        Iterator<? extends RevObject> iterator = objects.iterator();
        db.putAll(iterator, listener);
        counter.inserted = listener.inserted();
        counter.repeated = listener.found();
    }

}
