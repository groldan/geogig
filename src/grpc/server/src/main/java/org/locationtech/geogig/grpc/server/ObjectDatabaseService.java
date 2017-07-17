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
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static org.locationtech.geogig.grpc.common.Utils.EMPTY;
import static org.locationtech.geogig.grpc.common.Utils.newBlob;
import static org.locationtech.geogig.grpc.common.Utils.str;
import static org.locationtech.geogig.grpc.common.Utils.toConflict;
import static org.locationtech.geogig.grpc.common.Utils.toRpcConflict;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.locationtech.geogig.grpc.StringTuple;
import org.locationtech.geogig.grpc.storage.BatchResult;
import org.locationtech.geogig.grpc.storage.BlobMessage;
import org.locationtech.geogig.grpc.storage.ConflictMessage;
import org.locationtech.geogig.grpc.storage.ObjectDatabaseGrpc.ObjectDatabaseImplBase;
import org.locationtech.geogig.grpc.storage.ObjectInfo;
import org.locationtech.geogig.repository.Conflict;
import org.locationtech.geogig.storage.ConflictsDatabase;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.locationtech.geogig.storage.impl.TransactionBlobStore;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.protobuf.BoolValue;
import com.google.protobuf.Empty;
import com.google.protobuf.StringValue;
import com.google.protobuf.UInt64Value;

import io.grpc.stub.StreamObserver;

public class ObjectDatabaseService extends ObjectDatabaseImplBase {

    private ObjectStoreService objectStore;

    private Supplier<ObjectDatabase> _localdb;

    public ObjectDatabaseService(ObjectDatabase localdb) {
        this(Suppliers.ofInstance(localdb));
    }

    public ObjectDatabaseService(Supplier<ObjectDatabase> localdb) {
        checkNotNull(localdb);
        this.objectStore = new ObjectStoreService(localdb);
        this._localdb = localdb;
    }

    private ObjectDatabase localdb() {
        ObjectDatabase db = _localdb.get();
        checkNotNull(db);
        return db;
    }

    private ConflictsDatabase conflictsdb() {
        return localdb().getConflictsDatabase();
    }

    private TransactionBlobStore blobstore() {
        ObjectDatabase localdb = localdb();
        return (TransactionBlobStore) localdb.getBlobStore();
    }

    ////////////////////////////// ObjectStore ////////////////////////////////

    @Override
    public void exists(org.locationtech.geogig.grpc.model.ObjectId request,
            StreamObserver<BoolValue> responseObserver) {

        objectStore.exists(request, responseObserver);
    }

    @Override
    public void lookUp(StringValue request,
            StreamObserver<org.locationtech.geogig.grpc.model.ObjectId> responseObserver) {

        objectStore.lookUp(request, responseObserver);
    }

    @Override
    public StreamObserver<org.locationtech.geogig.grpc.model.RevObject> put(
            StreamObserver<BatchResult> responseObserver) {

        return objectStore.put(responseObserver);
    }

    @Override
    public StreamObserver<org.locationtech.geogig.grpc.model.ObjectId> get(
            StreamObserver<org.locationtech.geogig.grpc.model.RevObject> responseObserver) {

        return objectStore.get(responseObserver);
    }

    @Override
    public StreamObserver<org.locationtech.geogig.grpc.model.ObjectId> delete(
            StreamObserver<BatchResult> responseObserver) {

        return objectStore.delete(responseObserver);
    }

    @Override
    public StreamObserver<ObjectInfo> getObjects(StreamObserver<ObjectInfo> responseObserver) {

        return objectStore.getObjects(responseObserver);
    }

    ////////////////////////////// BlobStore ////////////////////////////////

    @Override
    public void getBlob(BlobMessage request, StreamObserver<BlobMessage> responseObserver) {
        try {
            String namespace = emptyToNull(request.getNamespace());
            String blobName = emptyToNull(request.getPath());
            Preconditions.checkNotNull(blobName);
            Optional<byte[]> blob = null == namespace ? blobstore().getBlob(blobName)
                    : blobstore().getBlob(namespace, blobName);

            BlobMessage response = request;
            if (blob.isPresent()) {
                response = newBlob(namespace, blobName, blob.get());
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void putBlob(org.locationtech.geogig.grpc.storage.BlobMessage request,
            io.grpc.stub.StreamObserver<Empty> responseObserver) {
        try {
            String namespace = emptyToNull(request.getNamespace());
            String blobName = emptyToNull(request.getPath());
            Preconditions.checkNotNull(blobName);
            byte[] blob = request.getContent().toByteArray();
            if (null == namespace) {
                blobstore().putBlob(blobName, blob);
            } else {
                blobstore().putBlob(namespace, blobName, blob);
            }
            responseObserver.onNext(EMPTY);
            responseObserver.onCompleted();
        } catch (Exception e) {
            e.printStackTrace();
            responseObserver.onError(e);
        }
    }

    @Override
    public void removeBlob(org.locationtech.geogig.grpc.storage.BlobMessage request,
            io.grpc.stub.StreamObserver<Empty> responseObserver) {
        try {
            String namespace = emptyToNull(request.getNamespace());
            String blobName = emptyToNull(request.getPath());
            Preconditions.checkNotNull(blobName);

            if (null == namespace) {
                blobstore().removeBlob(blobName);
            } else {
                blobstore().removeBlob(namespace, blobName);
            }
            responseObserver.onNext(EMPTY);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void removeBlobs(StringValue request,
            io.grpc.stub.StreamObserver<Empty> responseObserver) {
        try {
            String namespace = emptyToNull(request.getValue());
            Preconditions.checkNotNull(namespace);
            blobstore().removeBlobs(namespace);
            responseObserver.onNext(EMPTY);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    ////////////////////////////// ConflictsDatabase ////////////////////////////////

    @Override
    public void hasConflicts(StringValue request,
            io.grpc.stub.StreamObserver<BoolValue> responseObserver) {

        try {
            String namespace = request.isInitialized() ? request.getValue() : null;
            boolean hasConflicts = conflictsdb().hasConflicts(namespace);
            responseObserver.onNext(BoolValue.newBuilder().setValue(hasConflicts).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getConflictsCount(StringTuple request,
            io.grpc.stub.StreamObserver<UInt64Value> responseObserver) {

        try {
            String namespace = emptyToNull(request.getKey());
            String treePath = emptyToNull(request.getValue());
            long count = conflictsdb().getCountByPrefix(namespace, treePath);
            responseObserver.onNext(UInt64Value.newBuilder().setValue(count).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getConflict(org.locationtech.geogig.grpc.StringTuple request,
            io.grpc.stub.StreamObserver<org.locationtech.geogig.grpc.storage.Conflict> responseObserver) {

        try {
            String namespace = emptyToNull(request.getKey());
            String path = request.getValue();
            Preconditions.checkArgument(!isNullOrEmpty(path));
            Optional<Conflict> conflict = conflictsdb().getConflict(namespace, path);
            org.locationtech.geogig.grpc.storage.Conflict rpcConflict;
            rpcConflict = toRpcConflict(conflict.orNull());
            responseObserver.onNext(rpcConflict);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getConflicts(org.locationtech.geogig.grpc.StringTuple request,
            io.grpc.stub.StreamObserver<org.locationtech.geogig.grpc.storage.Conflict> responseObserver) {

        try {
            String namespace = emptyToNull(request.getKey());
            String prefixFilter = emptyToNull(request.getValue());
            Iterator<Conflict> iterator = conflictsdb().getByPrefix(namespace, prefixFilter);
            iterator.forEachRemaining((c) -> responseObserver.onNext(toRpcConflict(c)));
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void findConflicts(org.locationtech.geogig.grpc.storage.ScopedPaths request,
            io.grpc.stub.StreamObserver<StringValue> responseObserver) {

        try {
            String namespace = emptyToNull(request.getNamespace());

            Set<String> queryPaths = new HashSet<>();
            request.getPathsList().forEach((s) -> queryPaths.add(s));

            Set<String> conflictPaths = conflictsdb().findConflicts(namespace, queryPaths);

            conflictPaths.forEach((s) -> responseObserver.onNext(str(s)));
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public io.grpc.stub.StreamObserver<org.locationtech.geogig.grpc.storage.ConflictMessage> addConflicts(
            io.grpc.stub.StreamObserver<Empty> responseObserver) {

        StreamObserver<ConflictMessage> incomingObserver = new StreamObserver<ConflictMessage>() {

            private String namespace;

            private List<Conflict> conflicts;

            @Override
            public void onNext(ConflictMessage value) {
                if (namespace == null) {
                    namespace = value.getNamespace();
                    conflicts = new ArrayList<>();
                } else {
                    Preconditions.checkArgument(namespace.equals(value.getNamespace()));
                }

                Conflict c = toConflict(value.getConflict());
                Preconditions.checkNotNull(c);
                conflicts.add(c);
            }

            @Override
            public void onError(Throwable t) {
                responseObserver.onError(t);
            }

            @Override
            public void onCompleted() {
                if (conflicts != null) {
                    conflictsdb().addConflicts(namespace, conflicts);
                }
                responseObserver.onCompleted();
            }
        };

        return incomingObserver;
    }

    @Override
    public void removeConflicts(org.locationtech.geogig.grpc.storage.ScopedPaths request,
            io.grpc.stub.StreamObserver<Empty> responseObserver) {

        try {
            String namespace = emptyToNull(request.getNamespace());
            List<String> paths = new ArrayList<>();
            request.getPathsList().forEach((s) -> paths.add(s));
            conflictsdb().removeConflicts(namespace, paths);
            responseObserver.onNext(EMPTY);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void removeConflictsByPrefix(org.locationtech.geogig.grpc.StringTuple request,
            io.grpc.stub.StreamObserver<Empty> responseObserver) {

        try {
            String namespace = emptyToNull(request.getKey());
            String prefixFilter = emptyToNull(request.getValue());
            conflictsdb().removeByPrefix(namespace, prefixFilter);
            responseObserver.onNext(EMPTY);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

}
