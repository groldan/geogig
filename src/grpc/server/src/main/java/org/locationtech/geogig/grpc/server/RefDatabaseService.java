/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.grpc.server;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.locationtech.geogig.grpc.common.Utils.ref;
import static org.locationtech.geogig.grpc.common.Utils.str;

import java.util.Map;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.grpc.model.Ref;
import org.locationtech.geogig.grpc.storage.RefDatabaseGrpc;
import org.locationtech.geogig.storage.RefDatabase;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.protobuf.StringValue;

import io.grpc.stub.StreamObserver;

public class RefDatabaseService extends RefDatabaseGrpc.RefDatabaseImplBase {

    private Supplier<RefDatabase> _localdb;

    @VisibleForTesting
    public RefDatabaseService(RefDatabase localdb) {
        this(Suppliers.ofInstance(localdb));
    }

    public RefDatabaseService(Supplier<RefDatabase> localdb) {
        checkNotNull(localdb);
        this._localdb = localdb;
    }

    private RefDatabase localdb() {
        RefDatabase refDatabase = _localdb.get();
        checkNotNull(refDatabase);
        return refDatabase;
    }

    /**
     */
    @Override
    public void getAll(StringValue request,
            StreamObserver<org.locationtech.geogig.grpc.model.Ref> responseObserver) {

        @Nullable
        String prefix = request.getValue();
        try {
            Map<String, String> all;
            RefDatabase database = localdb();
            if (Strings.isNullOrEmpty(prefix)) {
                all = database.getAll();
            } else {
                all = database.getAll(prefix);
            }
            all.forEach((k, v) -> {
                boolean sym = v.startsWith("ref: ");
                Ref ref = ref(k, v, sym);
                responseObserver.onNext(ref);
            });
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            e.printStackTrace();
            responseObserver.onError(e);
        }
    }

    /**
     */
    @Override
    public void get(Ref request, StreamObserver<Ref> responseObserver) {

        try {
            String name = request.getName();
            boolean symref = request.getSymref();
            String value;
            if (symref) {
                value = localdb().getSymRef(name);
            } else {
                value = localdb().getRef(name);
            }
            responseObserver.onNext(ref(name, value, symref));
            responseObserver.onCompleted();
        } catch (IllegalArgumentException iae) {
            responseObserver.onNext(ref(null, null, false));
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            responseObserver.onError(e);
        }
    }

    /**
     */
    @Override
    public void put(org.locationtech.geogig.grpc.model.Ref request,
            StreamObserver<org.locationtech.geogig.grpc.model.Ref> responseObserver) {
        try {
            String name = request.getName();
            String value = request.getValue();
            boolean sym = request.getSymref();
            if (sym) {
                localdb().putSymRef(name, value);
            } else {
                localdb().putRef(name, value);
            }
            responseObserver.onNext(ref(name, value, sym));
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            responseObserver.onError(e);
        }
    }

    /**
     */
    @Override
    public void remove(StringValue request, StreamObserver<StringValue> responseObserver) {

        try {
            String name = request.getValue();
            String oldVal = localdb().remove(name);
            responseObserver.onNext(str(oldVal));
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            responseObserver.onError(e);
        }
    }

    /**
     */
    @Override
    public void removeAll(StringValue request,
            StreamObserver<org.locationtech.geogig.grpc.model.Ref> responseObserver) {

        try {
            String namespace = request.getValue();
            Map<String, String> removed = localdb().removeAll(namespace);
            removed.forEach((k, v) -> {
                boolean sym = v.startsWith("ref: ");
                Ref ref = ref(k, v, sym);
                responseObserver.onNext(ref);
            });
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            responseObserver.onError(e);
        }
    }
}
