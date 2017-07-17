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

import org.eclipse.jdt.annotation.Nullable;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.locationtech.geogig.grpc.repository.ObjectDatabaseClient;
import org.locationtech.geogig.grpc.server.ObjectDatabaseService;
import org.locationtech.geogig.storage.ConflictsDatabase;
import org.locationtech.geogig.storage.impl.ConflictsDatabaseConformanceTest;
import org.locationtech.geogig.storage.memory.HeapObjectDatabase;

import com.google.common.base.Throwables;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;

public class ObjectDatabaseClientGRPCConflictsTest
        extends ConflictsDatabaseConformanceTest<ConflictsDatabase> {

    private HeapObjectDatabase remotedb;

    private Server server;

    private ObjectDatabaseClient client;

    private ManagedChannel inProcessChannel;

    @Rule
    public TestName testName = new TestName();

    private String uniqueServerName;

    @Override
    protected ConflictsDatabase createConflictsDatabase() throws Exception {
        remotedb = new HeapObjectDatabase();
        remotedb.open();

        uniqueServerName = "in-process server for " + getClass() + "." + testName.getMethodName();

        InProcessServerBuilder serverbuilder = InProcessServerBuilder.forName(uniqueServerName)
                .directExecutor();

        serverbuilder.addService(new ObjectDatabaseService(remotedb));

        server = serverbuilder.build();
        try {
            server.start();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        inProcessChannel = InProcessChannelBuilder.forName(uniqueServerName).directExecutor()
                .build();

        boolean readOnly = false;
        client = new ObjectDatabaseClient(inProcessChannel, readOnly);
        client.open();
        return client.getConflictsDatabase();
    }

    @Override
    protected void dispose(@Nullable ConflictsDatabase conflicts) throws Exception {
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
