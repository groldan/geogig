/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.grpc.featureservice;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.store.ContentFeatureCollection;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.locationtech.geogig.grpc.client.Stubs;
import org.locationtech.geogig.grpc.featureservice.FeatureServiceDataStore;
import org.locationtech.geogig.grpc.server.FeatureService;
import org.locationtech.geogig.repository.Hints;
import org.locationtech.geogig.repository.Platform;
import org.locationtech.geogig.repository.Repository;
import org.locationtech.geogig.repository.RepositoryConnectionException;
import org.locationtech.geogig.test.TestData;
import org.locationtech.geogig.test.TestPlatform;
import org.locationtech.geogig.test.integration.TestContextBuilder;
import org.opengis.feature.simple.SimpleFeature;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;

/**
 * Test suite for {@link FeatureServiceDataStore}
 *
 */
public class FeatureServiceDataStoreTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Rule
    public TestName testName = new TestName();

    TestData testData;

    FeatureServiceDataStore dataStore;

    private Server server;

    private ManagedChannel inProcessChannel;

    public @Before void before() throws Exception {
        Repository repo = createRepo("repo1");
        testData = new TestData(repo);
        testData.init().loadDefaultData();

        String uniqueServerName = "in-process server for " + getClass() + "."
                + testName.getMethodName();
        InProcessServerBuilder serverbuilder = InProcessServerBuilder.forName(uniqueServerName)
                .directExecutor();

        serverbuilder.addService(new FeatureService(repo));

        server = serverbuilder.build();
        server.start();

        inProcessChannel = InProcessChannelBuilder.forName(uniqueServerName).directExecutor()
                .build();

        Stubs stubs = Stubs.create(inProcessChannel, true);
        stubs.open();
        dataStore = new FeatureServiceDataStore(stubs);
    }

    /**
     * Creates and returns a repo working off {@code <tmpFolder>/name} that's not open not does
     * exist (i.e. the init command hasn't been run on the empty folder)
     */
    private Repository createRepo(String name) throws RepositoryConnectionException, IOException {
        Platform platform = new TestPlatform(tmpFolder.getRoot());
        File pwd = tmpFolder.newFolder(name);
        platform.setWorkingDir(pwd);
        TestContextBuilder contextBuilder = new TestContextBuilder(platform);
        Hints hints = new Hints();
        Repository repo = contextBuilder.build(hints).repository();
        return repo;
    }

    public @After void after() {
        if (dataStore != null) {
            dataStore.dispose();
        }
    }

    public @Test void getFeatureTypeNames() throws IOException {
        String[] typeNames = dataStore.getTypeNames();
        assertNotNull(typeNames);
        Set<String> names = Sets.newHashSet(typeNames);
        assertEquals(Sets.newHashSet("Points", "Lines", "Polygons"), names);
    }

    public @Test void testGetBounds() throws IOException {
        ContentFeatureSource source = dataStore.getFeatureSource("Points");
        assertNotNull(source);
        ReferencedEnvelope bounds;
        bounds = source.getBounds();
        assertNotNull(bounds);
        System.err.println(bounds);

        Stopwatch sw = Stopwatch.createStarted();
        for (int i = 0; i < 100; i++) {
            bounds = source.getBounds();
            assertNotNull(bounds);
        }
        sw.stop();
        System.err.printf("time: %s, %s\n", sw, bounds);
    }

    public @Test void testGetFeatures() throws IOException {
        ContentFeatureSource source = dataStore.getFeatureSource("Points");
        assertNotNull(source);

        Query query = new Query();
        ContentFeatureCollection collection = source.getFeatures(query);
        for (int i = 0; i < 4; i++) {
            int count = traverseFully(collection);
            System.err.println(count);
        }
    }

    private int traverseFully(ContentFeatureCollection collection) {
        try (SimpleFeatureIterator it = collection.features()) {
            Stopwatch sw = Stopwatch.createStarted();
            System.err.println("traversing...");
            int count = 0;
            while (it.hasNext()) {
                SimpleFeature feature = it.next();
                count++;
            }
            sw.stop();
            System.err.printf("time: %s, %,d\n", sw, count);
            return count;
        }
    }
}
