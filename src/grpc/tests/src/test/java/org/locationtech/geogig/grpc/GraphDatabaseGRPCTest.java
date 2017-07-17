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
import org.junit.Rule;
import org.junit.rules.TestName;
import org.locationtech.geogig.grpc.repository.GraphDatabaseClient;
import org.locationtech.geogig.grpc.server.GraphDatabaseService;
import org.locationtech.geogig.repository.Platform;
import org.locationtech.geogig.storage.GraphDatabase;
import org.locationtech.geogig.storage.impl.GraphDatabaseTest;
import org.locationtech.geogig.storage.memory.HeapGraphDatabase;

import com.google.common.base.Throwables;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;

public class GraphDatabaseGRPCTest extends GraphDatabaseTest {

    private HeapGraphDatabase remotedb;

    private Server server;

    private GraphDatabaseClient client;

    private ManagedChannel inProcessChannel;

    @Rule
    public TestName testName = new TestName();

    private String uniqueServerName;

    @Override
    protected GraphDatabase createDatabase(Platform platform) throws Exception {
        remotedb = new HeapGraphDatabase(platform);
        remotedb.open();

        uniqueServerName = "in-process server for " + getClass() + "." + testName.getMethodName();

        InProcessServerBuilder serverbuilder = InProcessServerBuilder.forName(uniqueServerName)
                .directExecutor();

        serverbuilder.addService(new GraphDatabaseService(remotedb));

        server = serverbuilder.build();
        try {
            server.start();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }

        inProcessChannel = InProcessChannelBuilder.forName(uniqueServerName).directExecutor()
                .build();

        client = new GraphDatabaseClient(inProcessChannel);
        client.open();
        return client;
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
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
