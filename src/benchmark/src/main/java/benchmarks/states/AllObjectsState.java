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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Iterator;
import java.util.function.Supplier;

import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.RevCommit;
import org.locationtech.geogig.model.RevFeature;
import org.locationtech.geogig.model.RevFeatureType;
import org.locationtech.geogig.model.RevObject;
import org.locationtech.geogig.model.RevTree;
import org.locationtech.geogig.storage.BulkOpListener;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.locationtech.geogig.storage.impl.PersistedIterable;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;

/**
 * A state containing all the objects in the repository
 *
 */
@State(Scope.Benchmark)
public class AllObjectsState {

    private static final String FTS_KEY = "all_featuretypes";

    private static final String FEATURES_KEY = "all_features";

    private static final String TREES_KEY = "all_trees";

    private static final String BUCKETS_KEY = "all_buckets";

    private RepoState repo;

    private PersistedIterable<RevFeatureType> featureTypes;

    private PersistedIterable<RevTree> trees;

    private PersistedIterable<RevTree> buckets;

    private PersistedIterable<RevFeature> features;

    private PersistedIterable<RevCommit> revCommits;

    private Iterable<RevObject> all;

    private Iterator<RevObject> cycleIterator;

    public @Setup void initialize(SerializedState serializedState, AllContentIdsState allIds) {
        this.repo = allIds.repo();

        final ObjectDatabase db = repo.objectDatabase();

        featureTypes = openOrCreate(FTS_KEY, db, () -> allIds.getAllFeatureTypes(),
                RevFeatureType.class, serializedState);
        trees = openOrCreate(TREES_KEY, db, () -> allIds.getAllTrees(), RevTree.class,
                serializedState);
        buckets = openOrCreate(BUCKETS_KEY, db, () -> allIds.getAllBuckets(), RevTree.class,
                serializedState);
        features = openOrCreate(FEATURES_KEY, db, () -> allIds.getAllFeatures(), RevFeature.class,
                serializedState);

        revCommits = (PersistedIterable<RevCommit>) allIds.commits().getCommits();

        all = Iterables.concat(features, featureTypes, trees, buckets, revCommits);

        cycleIterator = Iterables.cycle(all).iterator();
    }

    public @TearDown void tearDown() {
        all = null;
        revCommits.close();
        features.close();
        featureTypes.close();
        trees.close();
        buckets.close();
    }

    private <T extends RevObject> PersistedIterable<T> openOrCreate(String key, ObjectDatabase db,
            Supplier<Iterable<ObjectId>> ids, Class<T> clazz, SerializedState serializedState) {

        final boolean persisted = serializedState.exists(key);
        @SuppressWarnings("unchecked")
        PersistedIterable<T> iterable = (PersistedIterable<T>) serializedState.get(key,
                SerializedState.REVOBJECT);

        if (persisted) {
            System.err.printf("## Reusing all pre-computed %s's from serialized file\n",
                    clazz.getSimpleName());
        } else {
            System.err.printf("\n## Querying ALL %s's in repo's object database...\n",
                    clazz.getSimpleName());

            Stopwatch sw = Stopwatch.createStarted();

            Iterator<T> it = db.getAll(ids.get(), BulkOpListener.NOOP_LISTENER, clazz);
            it.forEachRemaining((o) -> iterable.add(o));

            sw.stop();
            // System.err.printf(
            // "### RevObjects in repo: total=%,d, C=%,d, T=%,d, B=%,d, FT=%,d, F=%,d, time: %s\n",
            // iterable.size(), revCommits.size(), trees.size(), buckets.size(),
            // featureTypes.size(), features.size(), sw);
            System.err.printf(
                    "## Queried and and stored to serialized state all %,d %s's in repo in %s\n",
                    iterable.size(), clazz.getSimpleName(), sw);
            iterable.close();
        }
        return iterable;
    }

    public RepoState repo() {
        return repo;
    }

    public Iterable<RevObject> getAllObjects() {
        return all;
    }

    public Iterable<RevFeatureType> getAllFeatureTypes() {
        return featureTypes;
    }

    public Iterable<RevFeature> getAllFeatures() {
        return features;
    }

    public Iterable<RevTree> getAllTrees() {
        return trees;
    }

    public Iterable<RevTree> getAllBuckets() {
        return buckets;
    }

    public Iterable<RevCommit> getAllCommits() {
        return revCommits;
    }

    public InputStream getAllObjectsStream() throws IOException {
        InputStream fts = getAllFeatureTypesStream();
        InputStream ts = getAllTreesStream();
        InputStream bs = getAllBucketsStream();
        InputStream fs = getAllFeaturesStream();
        InputStream cs = getAllCommitsStream();

        InputStream concat = new SequenceInputStream(new SequenceInputStream(
                new SequenceInputStream(new SequenceInputStream(fts, ts), bs), fs), cs);
        concat = new BufferedInputStream(concat, 1024 * 1024);
        return concat;
    }

    public InputStream getAllCommitsStream() throws IOException {
        return revCommits.getAsStream();
    }

    public InputStream getAllFeaturesStream() throws IOException {
        return features.getAsStream();
    }

    public InputStream getAllBucketsStream() throws IOException {
        return buckets.getAsStream();
    }

    public InputStream getAllTreesStream() throws IOException {
        return trees.getAsStream();
    }

    public InputStream getAllFeatureTypesStream() throws IOException {
        return featureTypes.getAsStream();
    }

    private InputStream allObjectsInfiniteStream;

    public InputStream getAllObjectsInfiniteStream() throws IOException {
        if (allObjectsInfiniteStream == null) {

            InputStream infiniteStream = new InputStream() {
                private InputStream stream = getAllObjectsStream();

                public @Override int read() throws IOException {
                    int ret = stream.read();
                    if (-1 == ret) {
                        resetStream();
                        ret = stream.read();
                    }
                    return ret;
                }

                public @Override int read(byte b[], int off, int len) throws IOException {
                    int read = stream.read(b, off, len);
                    if (read == -1) {
                        resetStream();
                        return stream.read(b, off, len);
                    }
                    return read;
                }

                private void resetStream() throws IOException {
                    stream = getAllObjectsStream();
                }
            };

            allObjectsInfiniteStream = infiniteStream;

        }

        return allObjectsInfiniteStream;
    }

    public RevObject nextObject() {
        return cycleIterator.next();
    }
}
