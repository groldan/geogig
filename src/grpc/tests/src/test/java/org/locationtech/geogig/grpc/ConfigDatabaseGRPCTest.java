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

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.locationtech.geogig.grpc.repository.ConfigDatabaseClient;
import org.locationtech.geogig.grpc.server.ConfigDatabaseService;
import org.locationtech.geogig.repository.Platform;
import org.locationtech.geogig.storage.fs.IniFileConfigDatabase;
import org.locationtech.geogig.storage.impl.ConfigDatabaseTest;

import com.google.common.base.Throwables;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;

public class ConfigDatabaseGRPCTest extends ConfigDatabaseTest<ConfigDatabaseClient> {

    private IniFileConfigDatabase remotedb;

    private Server server;

    private ConfigDatabaseClient client;

    private ManagedChannel inProcessChannel;

    @Rule
    public TestName testName = new TestName();

    @Override
    protected ConfigDatabaseClient createDatabase(Platform platform) {
        remotedb = new IniFileConfigDatabase(platform);

        String uniqueServerName = "in-process server for " + getClass() + "."
                + testName.getMethodName();
        InProcessServerBuilder serverbuilder = InProcessServerBuilder.forName(uniqueServerName)
                .directExecutor();

        serverbuilder.addService(new ConfigDatabaseService(remotedb));

        server = serverbuilder.build();
        try {
            server.start();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }

        inProcessChannel = InProcessChannelBuilder.forName(uniqueServerName).directExecutor()
                .build();
        client = new ConfigDatabaseClient(inProcessChannel);
        return client;
    }

    @Override
    protected void destroy(ConfigDatabaseClient config) {
        if (inProcessChannel != null) {
            inProcessChannel.shutdownNow();
        }
        if (server != null) {
            server.shutdownNow();
        }
        if (remotedb != null) {
            try {
                remotedb.close();
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    @Override
    @Test
    @Ignore
    public void testNoRepository() {
        // intentionally empty
    }

    /**
     * Override as a no-op, since the pg config database's global settings don't depend on the
     * {@code $HOME/.geogigconfig} file.
     */
    @Override
    @Test
    @Ignore
    public void testNoUserHome() {
        // intentionally empty
    }

}
