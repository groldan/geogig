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

import static com.google.common.base.Preconditions.checkState;

import java.net.URI;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.grpc.repository.GRPCRepositoryResolver;
import org.locationtech.geogig.grpc.storage.ConfigDatabaseGrpc;
import org.locationtech.geogig.grpc.storage.ConfigDatabaseGrpc.ConfigDatabaseBlockingStub;
import org.locationtech.geogig.grpc.storage.GraphDatabaseGrpc;
import org.locationtech.geogig.grpc.storage.GraphDatabaseGrpc.GraphDatabaseBlockingStub;
import org.locationtech.geogig.grpc.storage.IndexDatabaseGrpc;
import org.locationtech.geogig.grpc.storage.IndexDatabaseGrpc.IndexDatabaseBlockingStub;
import org.locationtech.geogig.grpc.storage.IndexDatabaseGrpc.IndexDatabaseStub;
import org.locationtech.geogig.grpc.storage.ObjectDatabaseGrpc;
import org.locationtech.geogig.grpc.storage.ObjectDatabaseGrpc.ObjectDatabaseBlockingStub;
import org.locationtech.geogig.grpc.storage.ObjectDatabaseGrpc.ObjectDatabaseStub;
import org.locationtech.geogig.grpc.storage.RefDatabaseGrpc;
import org.locationtech.geogig.grpc.storage.RefDatabaseGrpc.RefDatabaseBlockingStub;
import org.locationtech.geogig.grpc.stream.FeatureServiceGrpc;
import org.locationtech.geogig.grpc.stream.FeatureServiceGrpc.FeatureServiceBlockingStub;
import org.locationtech.geogig.repository.Hints;

import com.google.common.annotations.VisibleForTesting;

import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.stub.MetadataUtils;

public class Stubs {

    private final URI repoURI;

    private final boolean readOnly;

    private final boolean channelProvided;

    private ManagedChannel channel;

    private @Nullable String repoName;

    private boolean open;

    Stubs(final URI repoURI, final boolean readOnly) {
        this.repoURI = repoURI;
        this.readOnly = readOnly;
        this.channelProvided = false;
    }

    @VisibleForTesting
    Stubs(ManagedChannel channel, boolean readOnly) {
        this.channel = channel;
        this.repoURI = null;
        this.readOnly = readOnly;
        this.channelProvided = true;
    }

    public static Stubs create(Hints hints) {
        URI uri = GRPCRepositoryResolver.getRPCRepositoryURI(hints);
        boolean readOnly = hints.getBoolean(Hints.OBJECTS_READ_ONLY);
        return new Stubs(uri, readOnly);
    }

    @VisibleForTesting
    public static Stubs create(ManagedChannel channel) {
        return create(channel, false);
    }

    @VisibleForTesting
    public static Stubs create(ManagedChannel channel, boolean readOnly) {
        return new Stubs(channel, readOnly);
    }

    public static Stubs forURI(URI repoURI) {
        return new Stubs(repoURI, false);
    }

    public void checkOpen() {
        checkState(isOpen(), "Database is closed");
    }

    public boolean isOpen() {
        return open;
    }

    public boolean open() {
        if (isOpen()) {
            return false;
        }
        if (!channelProvided) {
            this.channel = ManagedChannelPool.INSTANCE.acquire(repoURI);
            this.repoName = GRPCRepositoryResolver.INSTANCE.getName(repoURI);
        }
        open = true;
        return true;
    }

    public void close() {
        open = false;
        if (!channelProvided) {
            this.repoName = null;
            ManagedChannel channel = this.channel;
            this.channel = null;
            if (channel != null) {
                ManagedChannelPool.INSTANCE.release(channel);
            }
        }
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void checkWritable() throws IllegalStateException {
        checkOpen();
        checkState(!isReadOnly(), "Database is read only");
    }

    public ManagedChannel channel() {
        checkOpen();
        return channel;
    }

    private <T extends io.grpc.stub.AbstractStub<T>> T wrap(T stub) {
        if (repoName == null) {
            return stub;
        }
        Metadata extraHeaders = new Metadata();
        Key<String> key = Key.of("repo", Metadata.ASCII_STRING_MARSHALLER);
        @Nullable
        String value = repoName;
        extraHeaders.put(key, value);
        stub = MetadataUtils.attachHeaders(stub, extraHeaders);
        return stub;
    }

    public RefDatabaseBlockingStub newRefDatabaseBlockingStub() {
        RefDatabaseBlockingStub stub = RefDatabaseGrpc.newBlockingStub(channel);
        return wrap(stub);
    }

    public IndexDatabaseBlockingStub newIndexDatabaseBlockingStub() {
        return wrap(IndexDatabaseGrpc.newBlockingStub(channel()));
    }

    public IndexDatabaseStub newIndexDatabaseStub() {
        return wrap(IndexDatabaseGrpc.newStub(channel()));
    }

    public ObjectDatabaseBlockingStub newObjectDatabaseBlockingStub() {
        return wrap(ObjectDatabaseGrpc.newBlockingStub(channel()));
    }

    public ObjectDatabaseStub newObjectDatabaseStub() {
        return wrap(ObjectDatabaseGrpc.newStub(channel()));
    }

    public ConfigDatabaseBlockingStub newConfigDatabaseBlockingStub() {
        return wrap(ConfigDatabaseGrpc.newBlockingStub(channel()));
    }

    public GraphDatabaseBlockingStub newGraphDatabaseBlockingStub() {
        return wrap(GraphDatabaseGrpc.newBlockingStub(channel()));
    }

    public FeatureServiceBlockingStub newFeatureServiceBlockingStub() {
        return wrap(FeatureServiceGrpc.newBlockingStub(channel()));
    }
}
