/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package benchmarks.states;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.RevCommit;
import org.locationtech.geogig.porcelain.BranchListOp;
import org.locationtech.geogig.porcelain.LogOp;
import org.locationtech.geogig.storage.impl.PersistedIterable;
import org.locationtech.geogig.storage.impl.PersistedIterable.Serializer;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * A state containing all commits on a repository
 *
 */
@State(Scope.Benchmark)
public class AllCommitsState {

    private static final String SERIAL_KEY = "all_commits";

    private RepoState repo;

    private PersistedIterable<RevCommit> commits;

    private Set<ObjectId> commitIds;

    public @Setup void initialize(RepoState repo, SerializedState persistedState) {
        this.repo = repo;

        final boolean isPersisted = persistedState.exists(SERIAL_KEY);
        this.commitIds = new HashSet<>();
        this.commits = persistedState.get(SERIAL_KEY,
                (Serializer<RevCommit>) SerializedState.REVOBJECT);
        if (isPersisted) {
            load();
        } else {
            compute();
        }

    }

    public @TearDown void tearDown() {
        commits.close();
        commitIds.clear();
    }

    private void compute() {
        System.err.println("\n## Computing all commits in repo...");

        List<ObjectId> branches;
        branches = Lists.transform(repo.command(BranchListOp.class).call(), (b) -> b.getObjectId());
        Stopwatch sw = Stopwatch.createStarted();
        for (ObjectId id : branches) {
            Iterator<RevCommit> branchCommits = repo.command(LogOp.class).addCommit(id).call();
            while (branchCommits.hasNext()) {
                RevCommit c = branchCommits.next();
                if (commitIds.add(c.getId())) {
                    commits.add(c);
                }
            }
        }
        sw.stop();
        final int size = (int) commits.size();
        System.err.printf("## computed all commits in repo: count=%,d, time: %s\n", size, sw);

        commits.close();
        int size2 = Iterables.size(commits);
        Preconditions.checkState(size == size2);
    }

    private void load() {
        System.err.println("\n## Loading all commits from persisted file...");

        Stopwatch sw = Stopwatch.createStarted();
        for (RevCommit c : getCommits()) {
            if (!commitIds.add(c.getId())) {
                throw new IllegalStateException(String.format(
                        "Duplicated commit id %s, current size: %,d", c.getId(), commitIds.size()));
            }
        }
        sw.stop();
        System.err.printf("## Loaded pre-computed commits in repo: count=%,d, time: %s\n",
                commitIds.size(), sw);
    }

    public RepoState repo() {
        Preconditions.checkNotNull(repo);
        return repo;
    }

    public Set<ObjectId> getCommitIds() {
        return this.commitIds;
    }

    public Iterable<RevCommit> getCommits() {
        Preconditions.checkNotNull(commits);
        return commits;
    }

    public InputStream getCommitsAsStream() throws IOException {
        return commits.getAsStream();
    }
}
