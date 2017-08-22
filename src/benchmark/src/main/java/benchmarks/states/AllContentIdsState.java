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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.model.Bucket;
import org.locationtech.geogig.model.NodeRef;
import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.RevCommit;
import org.locationtech.geogig.model.RevObjects;
import org.locationtech.geogig.model.RevTree;
import org.locationtech.geogig.plumbing.diff.PreOrderDiffWalk;
import org.locationtech.geogig.plumbing.diff.PreOrderDiffWalk.AbstractConsumer;
import org.locationtech.geogig.plumbing.diff.PreOrderDiffWalk.BucketIndex;
import org.locationtech.geogig.plumbing.diff.PreOrderDiffWalk.Consumer;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.locationtech.geogig.storage.impl.PersistedIterable;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

/**
 * A state containing all the object ids in the repository
 *
 */
@State(Scope.Benchmark)
public class AllContentIdsState {

    private static final String COMMITS_KEY = "all_commit_ids";

    private static final String FTS_KEY = "all_featuretype_ids";

    private static final String FEATURES_KEY = "all_feature_ids";

    private static final String TREES_KEY = "all_tree_ids";

    private static final String BUCKETS_KEY = "all_bucket_ids";

    private RepoState repo;

    private AllCommitsState commitsState;

    private PersistedIterable<ObjectId> featureTypes;

    private PersistedIterable<ObjectId> trees;

    private PersistedIterable<ObjectId> buckets;

    private PersistedIterable<ObjectId> features;

    private PersistedIterable<ObjectId> commitIds;

    private Iterable<ObjectId> all;

    public @Setup void initialize(AllCommitsState commits, SerializedState persistedState) {
        this.repo = commits.repo();
        this.commitsState = commits;

        final boolean persisted = persistedState.exists(COMMITS_KEY);
        open(persistedState);

        if (!persisted) {
            createPreOrder(commits);
            featureTypes.close();
            trees.close();
            buckets.close();
            features.close();
            commitIds.close();
        }
    }

    private void open(SerializedState persistedState) {
        featureTypes = open(FTS_KEY, persistedState);
        features = open(FEATURES_KEY, persistedState);
        trees = open(TREES_KEY, persistedState);
        buckets = open(BUCKETS_KEY, persistedState);
        commitIds = open(COMMITS_KEY, persistedState);
        all = loadAll();
    }

    private Iterable<ObjectId> loadAll() {
        return Iterables.concat(featureTypes, features, trees, buckets, commitIds);
    }

    private PersistedIterable<ObjectId> open(String key, SerializedState persistedState) {
        PersistedIterable<ObjectId> iterable;
        iterable = persistedState.get(key, SerializedState.OBJECTID);
        return iterable;
    }

    private void createPreOrder(AllCommitsState commits) {
        System.err.println("\n## Computing all ObjectIds in repo...");

        final Map<ObjectId, RevCommit> allCommits;

        allCommits = Maps.uniqueIndex(commits.getCommits(), (c) -> c.getId());

        final ObjectDatabase db = repo.objectDatabase();

        Stopwatch sw = Stopwatch.createStarted();

        final Set<ObjectId> unique = new ConcurrentSkipListSet<>();

        for (RevCommit commit : allCommits.values()) {
            commitIds.add(commit.getId());
            List<ObjectId> parentIds = commit.getParentIds();
            if (parentIds.isEmpty()) {
                parentIds = ImmutableList.of(ObjectId.NULL);
            }
            long nt = trees.size();
            long nb = buckets.size();
            long nf = features.size();
            long nft = featureTypes.size();
            for (ObjectId parentId : parentIds) {
                ObjectId rootId = commit.getTreeId();
                ObjectId parentRootId = parentId.isNull() ? RevTree.EMPTY_TREE_ID
                        : allCommits.get(parentId).getTreeId();
                if (!rootId.equals(parentRootId)) {
                    RevTree newtree = db.getTree(rootId);
                    RevTree oldtree = db.getTree(parentRootId);
                    touchAllNewIds(oldtree, newtree, repo, unique);

                    nt = trees.size() - nt;
                    nb = buckets.size() - nb;
                    nf = features.size() - nf;
                    nft = featureTypes.size() - nft;
                    String spid = RevObjects.toString(parentId, 4, null).toString();
                    String scid = RevObjects.toString(commit.getId(), 4, null).toString();
                    System.err.printf("%s...%s: T: %,d, B: %,d, F: %,d, FT: %,d\n", spid, scid, nt,
                            nb, nf, nft);
                }
            }
        }
        sw.stop();
        unique.clear();
        all = loadAll();

        long totalSize = commitIds.size() + trees.size() + buckets.size() + features.size()
                + featureTypes.size();
        System.err.printf(
                "### ObjectIds in repo: total=%,d, C=%,d, T=%,d, B=%,d, FT=%,d, F=%,d, time: %s\n",
                totalSize, //
                commitIds.size(), //
                trees.size(), //
                buckets.size(), //
                featureTypes.size(), features.size(), //
                sw);

    }

    public AllCommitsState commits() {
        Preconditions.checkNotNull(this.commitsState);
        return this.commitsState;
    }

    public @TearDown void tearDown() {
        all = null;
        features.close();
        featureTypes.close();
        trees.close();
        buckets.close();
    }

    public RepoState repo() {
        return repo;
    }

    public Iterable<ObjectId> getAllObjects() {
        return all;
    }

    public Iterable<ObjectId> getAllFeatureTypes() {
        return featureTypes;
    }

    public Iterable<ObjectId> getAllFeatures() {
        return features;
    }

    public Iterable<ObjectId> getAllTrees() {
        return trees;
    }

    public Iterable<ObjectId> getAllBuckets() {
        return buckets;
    }

    public Iterable<ObjectId> getAllCommits() {
        return commitIds;
    }

    private void touchAllNewIds(RevTree left, RevTree right, RepoState repo,
            final Set<ObjectId> unique) {

        final ObjectDatabase odb = repo.objectDatabase();
        final boolean preserveIterationOrder = true;
        PreOrderDiffWalk walk = new PreOrderDiffWalk(left, right, odb, odb, preserveIterationOrder);

        Consumer newContentsConsumer = new AbstractConsumer() {

            private boolean add(NodeRef nr) {
                if (nr == null) {
                    return false;
                }
                ObjectId id = nr.getObjectId();
                boolean added = unique.add(id);
                return added;
            }

            private boolean add(ObjectId id) {
                if (id == null) {
                    return false;
                }
                boolean contains = unique.contains(id);
                boolean added = unique.add(id);
                return added;
            }

            @Override
            public boolean feature(@Nullable NodeRef oldNode, @Nullable NodeRef newNode) {
                if (add(newNode)) {
                    features.add(newNode.getObjectId());
                    ObjectId mdid = newNode.getNode().getMetadataId().orNull();
                    if (add(mdid)) {
                        featureTypes.add(mdid);
                    }
                }
                return true;
            }

            @Override
            public boolean tree(@Nullable NodeRef oldNode, @Nullable NodeRef newNode) {
                if (add(newNode)) {
                    trees.add(newNode.getObjectId());
                    ObjectId mdid = newNode.getNode().getMetadataId().orNull();
                    if (add(mdid)) {
                        featureTypes.add(mdid);
                    }
                    return true;
                }
                return false;
            }

            @Override
            public boolean bucket(NodeRef leftParent, NodeRef rightParent, BucketIndex bucketIndex,
                    @Nullable Bucket oldBucket, @Nullable Bucket newBucket) {

                boolean continueTraversal = newBucket != null && add(newBucket.getObjectId());
                if (continueTraversal) {
                    buckets.add(newBucket.getObjectId());
                }
                return continueTraversal;
            }
        };

        walk.walk(newContentsConsumer);
    }
}
