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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.Ref;
import org.locationtech.geogig.model.RevCommit;
import org.locationtech.geogig.porcelain.BranchListOp;
import org.locationtech.geogig.porcelain.LogOp;
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
import org.openjdk.jmh.infra.Blackhole;

import com.google.common.collect.ImmutableList;

import benchmarks.states.AllCommitsState;

@State(Scope.Benchmark)
@BenchmarkMode({ Mode.AverageTime })
@Warmup(iterations = 0)
@Measurement(iterations = 3)
@Threads(1)
@Fork(1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ResolveCommitGraph {

    @Param({ "true", "false" })
    boolean coldCache;

    /**
     * Time to resolve commit graph
     */
    @Benchmark
    public void log_BruteForce(Blackhole bh, AllCommitsState state) {
        if (coldCache) {
            state.repo().clearCaches();
        }
        ImmutableList<Ref> branches = state.repo().command(BranchListOp.class).call();
        Set<ObjectId> allCommits = new HashSet<>();
        for (Ref branch : branches) {
            ObjectId tipId = branch.getObjectId();
            Iterator<RevCommit> call = state.repo().command(LogOp.class)//
                    .addCommit(tipId)//
                    .setTopoOrder(false)//
                    .call();
            while (call.hasNext()) {
                RevCommit commit = call.next();
                bh.consume(commit);
                allCommits.add(commit.getId());
            }
        }
    }

    @Benchmark
    public void log_TopoOrder(Blackhole bh, AllCommitsState state) {
        if (coldCache) {
            state.repo().clearCaches();
        }
        ImmutableList<Ref> branches = state.repo().command(BranchListOp.class).call();
        Set<ObjectId> allCommits = new HashSet<>();
        for (Ref branch : branches) {
            ObjectId tipId = branch.getObjectId();
            Iterator<RevCommit> call = state.repo().command(LogOp.class)//
                    .addCommit(tipId)//
                    .setTopoOrder(true)//
                    .call();
            while (call.hasNext()) {
                RevCommit commit = call.next();
                bh.consume(commit);
                if (!allCommits.add(commit.getId())) {
                    break;// already visited, no need to keep going back its history
                }
            }
        }
    }
}
