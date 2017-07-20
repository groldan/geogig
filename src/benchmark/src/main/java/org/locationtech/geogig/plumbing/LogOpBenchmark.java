/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Johnathan Garret (Prominent Edge) - initial implementation
 */
package org.locationtech.geogig.plumbing;

import java.io.File;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.locationtech.geogig.cli.test.functional.CLITestContextBuilder;
import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.Ref;
import org.locationtech.geogig.model.RevCommit;
import org.locationtech.geogig.porcelain.BranchCreateOp;
import org.locationtech.geogig.porcelain.CheckoutOp;
import org.locationtech.geogig.porcelain.CommitOp;
import org.locationtech.geogig.porcelain.ConfigOp;
import org.locationtech.geogig.porcelain.ConfigOp.ConfigAction;
import org.locationtech.geogig.porcelain.LogOp;
import org.locationtech.geogig.repository.Context;
import org.locationtech.geogig.repository.Hints;
import org.locationtech.geogig.repository.Repository;
import org.locationtech.geogig.repository.impl.GeoGIG;
import org.locationtech.geogig.test.TestPlatform;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import com.google.common.collect.Lists;

@State(Scope.Benchmark)
@Fork(5)
public class LogOpBenchmark {

    protected File repositoryDirectory;

    protected GeoGIG geogig;

    protected Repository repo;

    @Param({ "100", "1000" })
    int numCommits;
    
    @Param({ "1", "5", "10" })
    int numBranches;
    
    @Param({ "true", "false" })
    boolean topoOrder;

    protected LogOp op;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        repositoryDirectory = File.createTempFile("repo", Long.toString(System.nanoTime()));
        repositoryDirectory.delete();
        repositoryDirectory.mkdirs();

        Context injector = createInjector();

        geogig = new GeoGIG(injector);
        repo = geogig.getOrCreateRepository();
        repo.command(ConfigOp.class).setAction(ConfigAction.CONFIG_SET).setName("user.name")
                .setValue("Gabriel Roldan").call();
        repo.command(ConfigOp.class).setAction(ConfigAction.CONFIG_SET).setName("user.email")
                .setValue("groldan@boundlessgeo.com").call();

        setupBranchesAndCommits(numBranches, numCommits);
    }

    @TearDown(Level.Trial)
    public void teardown() {
        geogig.close();
        geogig = null;
        repo = null;
    }

    protected Context createInjector() {
        TestPlatform platform = new TestPlatform(repositoryDirectory);
        URI uri = repositoryDirectory.getAbsoluteFile().toURI();
        Hints hints = new Hints().uri(uri).platform(platform);
        return new CLITestContextBuilder(platform).build(hints);
    }
    
    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @BenchmarkMode({ Mode.AverageTime })
    @Warmup(iterations = 5)
    @Measurement(iterations = 5)
    public void callAndIterate(Blackhole bh) {
        Iterator<RevCommit> commits = op.call();
        while (commits.hasNext()) {
            bh.consume(commits.next());
        }
    }

    private void setupBranchesAndCommits(int numBranches, int numCommits) throws Exception {
        List<ObjectId> ids = createBranches(numBranches, numCommits);

        op = geogig.command(LogOp.class);
        for (ObjectId id : ids) {
            op.addCommit(id);
        }
        op.setTopoOrder(topoOrder);
    }

    private RevCommit createCommits(int numCommits, String branchName) {
        RevCommit commit = null;
        for (int i = 1; i <= numCommits; i++) {
            commit = geogig.command(CommitOp.class).setAllowEmpty(true)
                    .setMessage("Commit " + i + " in branch " + branchName).call();
        }
        return commit;
    }

    private List<ObjectId> createBranches(int numBranches, int numCommits) {
        List<ObjectId> list = Lists.newArrayList();
        for (int i = 1; i <= numBranches; i++) {
            String branchName = "branch" + Integer.toString(i);
            geogig.command(CommitOp.class).setAllowEmpty(true)
                    .setMessage("Commit before " + branchName).call();
            geogig.command(BranchCreateOp.class).setAutoCheckout(true).setName(branchName).call();
            createCommits(numCommits / 2, branchName);
            geogig.command(CheckoutOp.class).setSource(Ref.MASTER).call();
            geogig.command(CommitOp.class).setAllowEmpty(true)
                    .setMessage("Commit during " + branchName).call();
            geogig.command(CheckoutOp.class).setSource(branchName).call();
            RevCommit lastCommit = createCommits(numCommits / 2, branchName);
            geogig.command(CheckoutOp.class).setSource(Ref.MASTER).call();
            list.add(lastCommit.getId());
        }
        return list;
    }
}
