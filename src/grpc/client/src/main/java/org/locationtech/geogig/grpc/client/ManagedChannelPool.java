/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.grpc.client;

import static org.locationtech.geogig.grpc.common.Constants.DEFAULT_PORT;

import java.net.URI;

import org.locationtech.geogig.grpc.repository.GRPCRepositoryResolver;
import org.locationtech.geogig.storage.impl.ConnectionManager;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class ManagedChannelPool extends ConnectionManager<URI, ManagedChannel> {

    public static final ManagedChannelPool INSTANCE = new ManagedChannelPool();

    @Override
    protected ManagedChannel connect(URI address) {
        GRPCRepositoryResolver.checkURI(address);

        String name = address.getHost();
        int port = address.getPort();
        if (-1 == port) {
            port = DEFAULT_PORT;
        }
        // System.err.printf("Connecting to grpc server %s:%d\n", name, port);
        ManagedChannel channel = ManagedChannelBuilder.forAddress(name, port)//
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to
                // avoid needing certificates.
                .usePlaintext(true)//
                .build();

        // System.err.println("opened channel " + channel);
        return channel;
    }

    @Override
    protected void disconnect(ManagedChannel connection) {
        // System.err.println("closing channel " + connection);
        connection.shutdownNow();
    }
}
