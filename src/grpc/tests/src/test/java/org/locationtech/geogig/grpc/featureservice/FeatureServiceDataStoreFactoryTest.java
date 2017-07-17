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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.locationtech.geogig.grpc.featureservice.FeatureServiceDataStoreFactory.DEFAULT_NAMESPACE;
import static org.locationtech.geogig.grpc.featureservice.FeatureServiceDataStoreFactory.HOST;
import static org.locationtech.geogig.grpc.featureservice.FeatureServiceDataStoreFactory.PORT;
import static org.locationtech.geogig.grpc.featureservice.FeatureServiceDataStoreFactory.REPOSITORY;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.geotools.data.DataAccessFactory.Param;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;
import org.geotools.data.DataStoreFinder;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

public class FeatureServiceDataStoreFactoryTest {

    Map<String, Serializable> params;

    public @Before void before() {
        params = new HashMap<>();
        params.put(HOST.key, "localhost");
        params.put(PORT.key, new Integer(5678));
        params.put(REPOSITORY.key, "reponame");

    }

    public @Test void getParametersInfo() throws IOException {
        FeatureServiceDataStoreFactory fac = new FeatureServiceDataStoreFactory();
        Param[] params = fac.getParametersInfo();
        assertNotNull(params);
        Set<Param> paramset = Sets.newHashSet(params);
        Set<Param> expected = Sets.newHashSet(DEFAULT_NAMESPACE, HOST, PORT, REPOSITORY);

        assertEquals(expected, paramset);
    }

    public @Test void testCanProcess() throws IOException {
        FeatureServiceDataStoreFactory fac = new FeatureServiceDataStoreFactory();
        assertTrue(fac.canProcess(params));

        params.remove(FeatureServiceDataStoreFactory.PORT.key);
        assertTrue(fac.canProcess(params));

        params.remove(FeatureServiceDataStoreFactory.HOST.key);
        assertFalse(fac.canProcess(params));

        params.put(FeatureServiceDataStoreFactory.HOST.key, "localhost");
        params.remove(FeatureServiceDataStoreFactory.REPOSITORY.key);
        assertFalse(fac.canProcess(params));
    }

    public @Test void testDataStoreFinder() {
        Iterator<DataStoreFactorySpi> availableDataStores = DataStoreFinder
                .getAvailableDataStores();
        while (availableDataStores.hasNext()) {
            DataStoreFactorySpi fac = availableDataStores.next();
            if (fac instanceof FeatureServiceDataStoreFactory) {
                assertTrue(true);
                return;
            }
        }
        fail(FeatureServiceDataStoreFactory.class.getName() + " not found through DataStoreFinder");
    }

    public @Test void testSPI() throws IOException {
        DataStore store = DataStoreFinder.getDataStore(params);
        assertNotNull(store);
        assertTrue(store instanceof FeatureServiceDataStore);
        store.dispose();

        params.put(FeatureServiceDataStoreFactory.PORT.key, new Integer(5678));
        store = DataStoreFinder.getDataStore(params);
        assertNotNull(store);
        assertTrue(store instanceof FeatureServiceDataStore);
        store.dispose();
    }

    public @Test void testNamespace() throws IOException {
        FeatureServiceDataStoreFactory fac = new FeatureServiceDataStoreFactory();
        FeatureServiceDataStore noNsStore = fac.createDataStore(params);

        assertNull(noNsStore.getNamespaceURI());

        final String ns = "http://geogig.org/grpc";
        params.put(DEFAULT_NAMESPACE.key, ns);
        FeatureServiceDataStore nsStore = fac.createDataStore(params);
        assertEquals(ns, nsStore.getNamespaceURI());
    }
}
