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
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.RevCommit;
import org.locationtech.geogig.model.RevObject;
import org.locationtech.geogig.storage.ObjectDatabase;
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
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import benchmarks.states.AllCommitsState;
import benchmarks.states.AllContentIdsState;
import benchmarks.states.RepoState;

/**
 * Benchmarks the optimal times for querying all objects reachable from the repository's revision
 * graph
 *
 */
@State(Scope.Benchmark)
@BenchmarkMode({ Mode.AverageTime })
@Warmup(iterations = 1)
@Measurement(iterations = 1)
@Threads(1)
@Fork(1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class QueryAllObjects {

    @Param({ /* "true", */"false" })
    boolean coldCache;

    /**
     * State for reporting auxiliary statistics
     */
    @State(Scope.Thread)
    @AuxCounters(Type.EVENTS)
    public static class Counter {
        long totalObjects;

        public long totalObjects() {
            return totalObjects;
        }
    }

    /**
     * What it takes to query and fetch all the {@link RevCommit} objects from the
     * {@link ObjectDatabase} under the ideal conditions that we already have all the object ids for
     * all commits.
     */
    @Benchmark
    public void queryAllCommits(AllCommitsState state, Counter counter) {
        if (coldCache) {
            state.repo().clearCaches();
        }
        counter.totalObjects = 0;
        ObjectDatabase db = state.repo().objectDatabase();
        Set<ObjectId> ids = state.getCommitIds();
        Iterator<RevObject> result = db.getAll(ids);
        int size = Iterators.size(result);
        counter.totalObjects = size;
        System.err.printf("Queried all %,d commits\n", size);
        Preconditions.checkState(size == ids.size(), "Expected %s, got %s", ids.size(), size);
    }

    /**
     * What is takes to query and consume all {@link RevObject}s from the {@link ObjectDatabase} in
     * one shot under the ideal conditions that we already have all the object ids for all the
     * objects in the database
     */
    @Benchmark
    public void queryAllContents(AllContentIdsState state, Counter counter) {
        if (coldCache) {
            state.repo().clearCaches();
        }
        counter.totalObjects = 0;
        Iterable<ObjectId> allObjectIds = state.getAllObjects();
        RepoState repo = state.repo();
        ObjectDatabase objectDatabase = repo.objectDatabase();
        Iterator<RevObject> allObjects = objectDatabase.getAll(allObjectIds);
        int size = Iterators.size(allObjects);
        counter.totalObjects = size;
        System.err.printf("Queried all %,d objects\n", size);
        // Preconditions.checkState(size == allObjectIds.size(), "Expected %s, got %s",
        // allObjectIds.size(), size);
    }

    /**
     * What it takes to query all reachable contents (except commits themselves)from all commits
     * under the ideal condition that all the reachable content ids have already been resolved.
     */
    @Benchmark
    public void queryAllCommitContents(AllContentIdsState state, Counter counter) {
        if (coldCache) {
            state.repo().clearCaches();
        }
        counter.totalObjects = 0;

        Iterable<ObjectId> allButCommits = Iterables.concat(state.getAllTrees(),
                state.getAllBuckets(), state.getAllFeatureTypes(), state.getAllFeatures());

        RepoState repo = state.repo();
        ObjectDatabase objectDatabase = repo.objectDatabase();
        Iterator<RevObject> allObjects = objectDatabase.getAll(allButCommits);
        int size = Iterators.size(allObjects);
        counter.totalObjects = size;
        System.err.printf("Queried all %,d objects\n", size);
        // final int expected = state.getAllTrees().size() + state.getAllBuckets().size()
        // + state.getAllFeatureTypes().size() + state.getAllFeatures().size();
        //
        // Preconditions.checkState(size == expected, "Expected %s, got %s", expected, size);
    }
}
