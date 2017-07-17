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
import org.locationtech.geogig.grpc.repository.IndexDatabaseClient;
import org.locationtech.geogig.grpc.server.IndexDatabaseService;
import org.locationtech.geogig.repository.Hints;
import org.locationtech.geogig.repository.Platform;
import org.locationtech.geogig.storage.IndexDatabase;
import org.locationtech.geogig.storage.impl.IndexDatabaseConformanceTest;
import org.locationtech.geogig.storage.memory.HeapIndexDatabase;

import com.google.common.base.Throwables;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;

public class IndexDatabaseGRPCTest extends IndexDatabaseConformanceTest {

    private HeapIndexDatabase remotedb;

    private Server server;

    private IndexDatabaseClient client;

    private ManagedChannel inProcessChannel;

    @Rule
    public TestName testName = new TestName();

    private String uniqueServerName;

    @Before
    @Override
    public void setUp() throws IOException {
        remotedb = new HeapIndexDatabase();
        remotedb.open();

        uniqueServerName = "in-process server for " + getClass() + "." + testName.getMethodName();

        InProcessServerBuilder serverbuilder = InProcessServerBuilder.forName(uniqueServerName)
                .directExecutor();

        serverbuilder.addService(new IndexDatabaseService(remotedb));

        server = serverbuilder.build();
        try {
            server.start();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        super.setUp();
    }

    @Override
    protected IndexDatabase createIndexDatabase(Platform platform, Hints hints) {
        inProcessChannel = InProcessChannelBuilder.forName(uniqueServerName).directExecutor()
                .build();

        boolean readOnly = hints.getBoolean(Hints.OBJECTS_READ_ONLY);
        client = new IndexDatabaseClient(inProcessChannel, readOnly);
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
