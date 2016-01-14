/* Copyright (c) 2016 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Erik Merkle (Boundless) - initial implementation
 */
package org.locationtech.geogig.test.performance;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.locationtech.geogig.api.ObjectId;
import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.api.RevObject;
import org.locationtech.geogig.api.TestPlatform;
import org.locationtech.geogig.api.plumbing.diff.RevObjectTestSupport;
import org.locationtech.geogig.repository.Hints;
import org.locationtech.geogig.storage.ObjectStore;
import org.locationtech.geogig.storage.ObjectStoreConformanceTest;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Base test class for verifying an {@link org.locationtech.geogig.storage.ObjectStore ObjectStore}
 * doesn't suffer from any concurrent read/write issues.
 */
public abstract class ObjectStoreConcurrencyTest {

    /**
     * Number of Threads to use in the ExecutorServices.
     */
    private static final int CONCURRENT_THREAD_COUNT = 4;

    /**
     * Number of RevObjects to generate/insert on a given concurrent thread.
     */
    private static final int CONCURRENT_INSERT_COUNT = 20;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private TestPlatform platform;

    private Hints hints;

    private ObjectStore db;

    private final RevObjectTestSupport objects = new RevObjectTestSupport();

    private ExecutorService dbWriteThreads;

    @Before
    public void setUp() throws IOException {
        File root = folder.getRoot();
        folder.newFolder(".geogig");
        File home = folder.newFolder("home");
        platform = new TestPlatform(root);
        platform.setUserHome(home);
        hints = new Hints();

        this.db = createOpen(platform, hints);
        this.db.open();

        dbWriteThreads = Executors.newFixedThreadPool(CONCURRENT_THREAD_COUNT,
            new ThreadFactoryBuilder().setNameFormat("db-put-thread-%d").build());
    }

    @After
    public void after() {
        if (db != null) {
            db.close();
        }
        if (dbWriteThreads != null) {
            dbWriteThreads.shutdownNow();
        }
    }

    protected abstract ObjectStore createOpen(Platform platform, Hints hints);

    @Test
    public void testConcurrentPutAndGet() throws InterruptedException, ExecutionException {
        final List<RevObject> expected = fillRevObjectList(0);

        List<Future<Boolean>> puts = this.concurrentGetAndPut(expected);
        for (Future<Boolean> f : puts) {
            assertTrue("Expected PUT to return TRUE", f.get());
        }
    }

    @Test
    public void testConcurrentPutAllAndGet() throws InterruptedException, ExecutionException {
        List<RevObject> initialObjects = fillRevObjectList(0);
        List<Future<Boolean>> initialFutures = concurrentGetAndPut(initialObjects);
        for (Future<Boolean> future : initialFutures) {
            assertTrue("Expected PUT to return TRUE", future.get());
        }
        ImmutableList.Builder<List<RevObject>> listBuilder = new ImmutableList.Builder<>();
        for (int i = 0; i < CONCURRENT_INSERT_COUNT; ++i) {
            List<RevObject> subList = fillRevObjectList((i + 1) * CONCURRENT_INSERT_COUNT);
            listBuilder.add(subList);
        }
        List<List<RevObject>> revObjectLists = listBuilder.build();
        List<Future<Void>> futures = concurrentGetAndPutAll(revObjectLists);
        for (Future<Void> f : futures) {
            f.get();
        }
        for (List<RevObject> subList : revObjectLists) {
            for (RevObject revObject : subList) {
                assertTrue("Expected Object to be present", db.exists(revObject.getId()));
            }
        }
    }

    @Test
    public void testConcurrentDeleteAllAndGet() throws InterruptedException, ExecutionException {
        List<RevObject> initialObjects = fillRevObjectList(0);
        List<Future<Boolean>> initialFutures = concurrentGetAndPut(initialObjects);
        for (Future<Boolean> future : initialFutures) {
            assertTrue("Expected PUT to return TRUE", future.get());
        }
        ImmutableList.Builder<List<RevObject>> listBuilder = new ImmutableList.Builder<>();
        for (int i = 0; i < CONCURRENT_INSERT_COUNT; ++i) {
            List<RevObject> subList = fillRevObjectList((i + 1) * CONCURRENT_INSERT_COUNT);
            listBuilder.add(subList);
        }
        List<List<RevObject>> revObjectLists = listBuilder.build();
        List<List<ObjectId>> objectIdLists = Lists.transform(revObjectLists,
            new Function<List<RevObject>, List<ObjectId>>() {
            @Override
            public List<ObjectId> apply(List<RevObject> revObjectList) {
                return Lists.transform(revObjectList, new Function<RevObject, ObjectId>() {
                    @Override
                    public ObjectId apply(RevObject revObject) {
                        return revObject.getId();
                    }

                });
            }

        });
        List<Future<Void>> futures = concurrentDeleteAllAndGet(objectIdLists, initialObjects);
        for (Future<Void> f : futures) {
            f.get();
        }
    }

    /**
     * Returns a List of randomly generated fake RevObjects. The supplied offset value should be 0
     * if this method is used only once to generate a list of RevObjects. If this method is used
     * more than once (for example, to build a larger list of random RevObjects), subsequent calls
     * to this method should pass an offset that is equal to the N times
     * {@link ObjectStoreConformanceTest#CONCURRENT_INSERT_COUNT CONCURRENT_INSERT_COUNT}, where N
     * is the number of times this method has already been called to ensure the RevObjects created
     * do not clobber previously created RevObjects.
     *
     * @param offset The offset for generating fake IDs to use in generating the RevObjects.
     * @return A List of fake RevObjects with sequential fake IDs, starting with the supplied
     * offset.
     */
    private List<RevObject> fillRevObjectList(int offset) {
        ImmutableList.Builder<RevObject> list = new ImmutableList.Builder<>();
        for (int i = 0 + offset; i < CONCURRENT_INSERT_COUNT + offset; i++) {
            list.add(objects.feature(i, randomString(), randomString(), randomString()));
        }
        return list.build();
    }

    private String randomString() {
        return UUID.randomUUID().toString();
    }

    private List<Future<Boolean>> concurrentGetAndPut(List<RevObject> expected) {
        ImmutableList.Builder<Future<Boolean>> futures = new ImmutableList.Builder<>();
        Iterator<RevObject> iterator = expected.iterator();
        while (iterator.hasNext()) {
            final RevObject obj = iterator.next();
            futures.add(dbWriteThreads.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    Boolean returnVal = Boolean.valueOf(db.put(obj));
                    db.get(obj.getId());
                    return returnVal;
                }
            }));
        }
        return futures.build();
    }

    private List<Future<Void>> concurrentGetAndPutAll(List<? extends List<RevObject>> puts) {
        ImmutableList.Builder<Future<Void>> futures = new ImmutableList.Builder<>();
        Iterator<? extends List<RevObject>> iterator = puts.iterator();
        while (iterator.hasNext()) {
            final List<RevObject> list = iterator.next();
            final ObjectId randomId1 = list.get(new Random().nextInt(list.size())).getId();
            final ObjectId randomId2 = list.get(new Random().nextInt(list.size())).getId();
            futures.add(dbWriteThreads.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    db.exists(randomId1);
                    db.putAll(list.iterator());
                    db.get(randomId2);
                    return null;
                }

            }));
        }
        return futures.build();
    }

    private List<Future<Void>> concurrentDeleteAllAndGet(List<? extends List<ObjectId>> deletes,
        List<RevObject> reads) {
        ImmutableList.Builder<Future<Void>> futures = new ImmutableList.Builder<>();
        Iterator<? extends List<ObjectId>> iterator = deletes.iterator();
        while (iterator.hasNext()) {
            final List<ObjectId> list = iterator.next();
            final ObjectId randomId1 = list.get(new Random().nextInt(list.size()));
            final ObjectId randomId2 = reads.get(new Random().nextInt(reads.size())).getId();
            futures.add(dbWriteThreads.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    db.exists(randomId1);
                    db.deleteAll(list.iterator());
                    db.get(randomId2);
                    return null;
                }

            }));
        }
        return futures.build();
    }
}
