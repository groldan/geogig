/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.grpc.repository;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static org.locationtech.geogig.grpc.common.Utils.ref;
import static org.locationtech.geogig.grpc.common.Utils.str;
import static org.locationtech.geogig.grpc.common.Utils.toMap;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.grpc.client.Stubs;
import org.locationtech.geogig.grpc.model.Ref;
import org.locationtech.geogig.grpc.storage.RefDatabaseGrpc.RefDatabaseBlockingStub;
import org.locationtech.geogig.repository.Hints;
import org.locationtech.geogig.repository.RepositoryConnectionException;
import org.locationtech.geogig.storage.RefDatabase;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.google.protobuf.StringValue;

import io.grpc.ManagedChannel;

public class RefDatabaseClient implements RefDatabase {

    private RefDatabaseBlockingStub remotedb;

    private final Stubs stubs;

    @Inject
    public RefDatabaseClient(Hints hints) {
        this.stubs = Stubs.create(hints);
    }

    @VisibleForTesting
    public RefDatabaseClient(ManagedChannel channel) {
        this.stubs = Stubs.create(channel);
    }

    @Override
    public void lock() throws TimeoutException {

        // TODO Auto-generated method stub

    }

    @Override
    public void unlock() {
        // TODO Auto-generated method stub

    }

    @Override
    public void configure() throws RepositoryConnectionException {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean checkConfig() throws RepositoryConnectionException {
        // TODO Auto-generated method stub
        return true;
    }

    @Override
    public void create() {
        if (stubs.open()) {
            this.remotedb = stubs.newRefDatabaseBlockingStub();
        }
    }

    @Override
    public void close() {
        stubs.close();
        this.remotedb = null;
    }

    @Override
    public @Nullable String getRef(String name) {
        Preconditions.checkNotNull(name, "name can't be null");
        Ref ref = remotedb.get(ref(name, null, false));
        if (isNullOrEmpty(ref.getName())) {
            throw new IllegalArgumentException(String.format("%s is a symbolic ref", name));
        }

        String value = ref.getValue();
        return Strings.isNullOrEmpty(value) ? null : value;
    }

    @Override
    public @Nullable String getSymRef(String name) {
        Preconditions.checkNotNull(name, "name can't be null");
        Ref ref = remotedb.get(ref(name, null, true));
        if (isNullOrEmpty(ref.getName())) {
            throw new IllegalArgumentException(String.format("%s is not a symbolic ref", name));
        }
        Preconditions.checkArgument(ref.getSymref(), "%s is not a symbolic ref", ref.getName());
        String value = ref.getValue();
        return Strings.isNullOrEmpty(value) ? null : value;
    }

    @Override
    public void putRef(String refName, String refValue) {
        Ref request = ref(refName, refValue, false);
        remotedb.put(request);
    }

    @Override
    public void putSymRef(String name, String val) {
        Ref request = ref(name, val, true);
        remotedb.put(request);
    }

    @Override
    public @Nullable String remove(String refName) {
        checkNotNull(refName);
        StringValue oldval = remotedb.remove(str(refName));
        return oldval.getValue();
    }

    @Override
    public Map<String, String> getAll() {
        return getAllInternal(null);
    }

    @Override
    public Map<String, String> getAll(String prefix) {
        Preconditions.checkNotNull(prefix, "namespace can't be null");
        return getAllInternal(prefix);
    }

    public Map<String, String> getAllInternal(@Nullable String prefix) {
        Iterator<Ref> all = remotedb.getAll(str(prefix));
        Map<String, String> result = toMap(all);
        return result;
    }

    @Override
    public Map<String, String> removeAll(String namespace) {
        Preconditions.checkNotNull(namespace, "provided namespace is null");
        Iterator<Ref> removed = remotedb.removeAll(str(namespace));
        return toMap(removed);
    }

}
