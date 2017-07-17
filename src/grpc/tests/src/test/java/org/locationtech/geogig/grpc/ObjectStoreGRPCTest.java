/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.grpc;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.locationtech.geogig.grpc.repository.ObjectStoreClient;
import org.locationtech.geogig.grpc.server.ObjectStoreService;
import org.locationtech.geogig.repository.Hints;
import org.locationtech.geogig.repository.Platform;
import org.locationtech.geogig.storage.ObjectStore;
import org.locationtech.geogig.storage.impl.ObjectStoreConformanceTest;
import org.locationtech.geogig.storage.memory.HeapObjectStore;

import com.google.common.base.Throwables;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;

public class ObjectStoreGRPCTest extends ObjectStoreConformanceTest {

    private HeapObjectStore remotedb;

    private Server server;

    private ObjectStoreClient client;

    private ManagedChannel inProcessChannel;

    @Rule
    public TestName testName = new TestName();

    private String uniqueServerName;

    @Before
    @Override
    public void setUp() throws IOException {
        remotedb = new HeapObjectStore();
        remotedb.open();

        uniqueServerName = "in-process server for " + getClass() + "." + testName.getMethodName();

        InProcessServerBuilder serverbuilder = InProcessServerBuilder.forName(uniqueServerName)
                .directExecutor();

        serverbuilder.addService(new ObjectStoreService(remotedb));

        server = serverbuilder.build();
        try {
            server.start();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        super.setUp();
    }

    @Override
    protected ObjectStore createOpen(Platform platform, Hints hints) {
        inProcessChannel = InProcessChannelBuilder.forName(uniqueServerName).directExecutor()
                .build();

        boolean readOnly = hints.getBoolean(Hints.OBJECTS_READ_ONLY);
        client = new ObjectStoreClient(inProcessChannel, readOnly);
        client.open();
        return client;
    }

    @After
    public void dispose() {
        if (inProcessChannel != null) {
            inProcessChannel.shutdownNow();
        }
        if (server != null) {
            server.shutdownNow();
        }
        if (remotedb != null) {
            remotedb.close();
        }
    }
}
