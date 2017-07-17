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
import static org.locationtech.geogig.grpc.common.Utils.EMPTY;
import static org.locationtech.geogig.grpc.common.Utils.id;
import static org.locationtech.geogig.grpc.common.Utils.newIndexInfo;
import static org.locationtech.geogig.grpc.common.Utils.newRpcIndexInfo;

import java.util.List;
import java.util.Map;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.grpc.storage.BatchResult;
import org.locationtech.geogig.grpc.storage.IndexDatabaseGrpc.IndexDatabaseImplBase;
import org.locationtech.geogig.grpc.storage.ObjectInfo;
import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.repository.IndexInfo;
import org.locationtech.geogig.repository.IndexInfo.IndexType;
import org.locationtech.geogig.storage.IndexDatabase;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.BoolValue;
import com.google.protobuf.Empty;
import com.google.protobuf.StringValue;

import io.grpc.stub.StreamObserver;

public class IndexDatabaseService extends IndexDatabaseImplBase {

    private ObjectStoreService objectStore;

    private Supplier<IndexDatabase> _localdb;

    @VisibleForTesting
    public IndexDatabaseService(IndexDatabase localdb) {
        this(Suppliers.ofInstance(localdb));
    }

    public IndexDatabaseService(Supplier<IndexDatabase> localdb) {
        checkNotNull(localdb);
        this.objectStore = new ObjectStoreService(localdb);
        this._localdb = localdb;
    }

    private IndexDatabase localdb() {
        IndexDatabase database = _localdb.get();
        checkNotNull(database);
        return database;
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

    ////////////////////////////// IndexDatabase ////////////////////////////////

    @Override
    public void createIndexInfo(org.locationtech.geogig.grpc.storage.IndexInfo request,
            io.grpc.stub.StreamObserver<org.locationtech.geogig.grpc.storage.IndexInfo> responseObserver) {

        try {
            IndexInfo info = newIndexInfo(request);
            String treeName = info.getTreeName();
            String attributeName = info.getAttributeName();
            IndexType strategy = info.getIndexType();
            Map<String, Object> metadata = info.getMetadata();
            IndexInfo created;
            created = localdb().createIndexInfo(treeName, attributeName, strategy, metadata);
            org.locationtech.geogig.grpc.storage.IndexInfo result = newRpcIndexInfo(created);
            responseObserver.onNext(result);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void updateIndexInfo(org.locationtech.geogig.grpc.storage.IndexInfo request,
            io.grpc.stub.StreamObserver<org.locationtech.geogig.grpc.storage.IndexInfo> responseObserver) {

        try {
            IndexInfo info = newIndexInfo(request);
            String treeName = info.getTreeName();
            String attributeName = info.getAttributeName();
            IndexType strategy = info.getIndexType();
            Map<String, Object> metadata = info.getMetadata();
            IndexInfo updated;
            updated = localdb().updateIndexInfo(treeName, attributeName, strategy, metadata);
            org.locationtech.geogig.grpc.storage.IndexInfo result = newRpcIndexInfo(updated);
            responseObserver.onNext(result);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getIndexInfos(org.locationtech.geogig.grpc.StringTuple request,
            io.grpc.stub.StreamObserver<org.locationtech.geogig.grpc.storage.IndexInfo> responseObserver) {
        try {
            @Nullable
            String treeName = emptyToNull(request.getKey());
            @Nullable
            String attributeName = emptyToNull(request.getValue());

            List<IndexInfo> indexInfos;
            if (treeName == null && attributeName == null) {
                indexInfos = localdb().getIndexInfos();
            } else if (attributeName == null) {
                indexInfos = localdb().getIndexInfos(treeName);
            } else {
                Optional<IndexInfo> indexInfo = localdb().getIndexInfo(treeName, attributeName);
                if (indexInfo.isPresent()) {
                    indexInfos = ImmutableList.of(indexInfo.get());
                } else {
                    indexInfos = ImmutableList.of();
                }
            }
            for (IndexInfo info : indexInfos) {
                org.locationtech.geogig.grpc.storage.IndexInfo rpcIndexInfo = newRpcIndexInfo(info);
                responseObserver.onNext(rpcIndexInfo);
            }
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void dropIndexInfo(org.locationtech.geogig.grpc.storage.IndexInfo request,
            io.grpc.stub.StreamObserver<BoolValue> responseObserver) {

        try {
            IndexInfo index = newIndexInfo(request);
            boolean dropped = localdb().dropIndex(index);
            responseObserver.onNext(BoolValue.newBuilder().setValue(dropped).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void clearIndex(org.locationtech.geogig.grpc.storage.IndexInfo request,
            io.grpc.stub.StreamObserver<Empty> responseObserver) {
        try {
            IndexInfo index = newIndexInfo(request);
            localdb().clearIndex(index);
            responseObserver.onNext(EMPTY);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void addIndexedTree(org.locationtech.geogig.grpc.storage.IndexedTreeInfo request,
            io.grpc.stub.StreamObserver<Empty> responseObserver) {
        try {
            IndexInfo index = newIndexInfo(request.getIndexInfo());
            ObjectId originalTree = id(request.getOriginalTree());
            ObjectId indexedTree = id(request.getIndexedTree());

            localdb().addIndexedTree(index, originalTree, indexedTree);
            responseObserver.onNext(EMPTY);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void resolveIndexedTree(org.locationtech.geogig.grpc.storage.IndexedTreeInfo request,
            io.grpc.stub.StreamObserver<org.locationtech.geogig.grpc.model.ObjectId> responseObserver) {
        try {
            IndexInfo index = newIndexInfo(request.getIndexInfo());
            ObjectId treeId = id(request.getOriginalTree());
            Optional<ObjectId> indexedTree = localdb().resolveIndexedTree(index, treeId);
            org.locationtech.geogig.grpc.model.ObjectId response = id(indexedTree.orNull());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}
