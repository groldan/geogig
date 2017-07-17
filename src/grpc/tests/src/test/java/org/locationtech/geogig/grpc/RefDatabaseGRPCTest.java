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

import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.locationtech.geogig.grpc.repository.RefDatabaseClient;
import org.locationtech.geogig.grpc.server.RefDatabaseService;
import org.locationtech.geogig.repository.Platform;
import org.locationtech.geogig.storage.ConfigDatabase;
import org.locationtech.geogig.storage.RefDatabase;
import org.locationtech.geogig.storage.fs.FileRefDatabase;
import org.locationtech.geogig.storage.fs.IniFileConfigDatabase;
import org.locationtech.geogig.test.integration.repository.RefDatabaseTest;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;

public class RefDatabaseGRPCTest extends RefDatabaseTest {

    private FileRefDatabase remotedb;

    private Server server;

    private RefDatabaseClient client;

    private ManagedChannel inProcessChannel;
 
    @Rule
    public TestName testName = new TestName();

    @Override
    protected RefDatabase createDatabase(Platform platform) throws Exception {
        ConfigDatabase configDB = new IniFileConfigDatabase(platform);
        remotedb = new FileRefDatabase(platform, configDB, null);
        remotedb.create();

        String uniqueServerName = "in-process server for " + getClass() + "."
                + testName.getMethodName();
        InProcessServerBuilder serverbuilder = InProcessServerBuilder.forName(uniqueServerName)
                .directExecutor();

        serverbuilder.addService(new RefDatabaseService(remotedb));

        server = serverbuilder.build();
        server.start();

        inProcessChannel = InProcessChannelBuilder.forName(uniqueServerName).directExecutor()
                .build();

        client = new RefDatabaseClient(inProcessChannel);
        client.create();
        return client;
    }

    @After
    public void after() {
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
