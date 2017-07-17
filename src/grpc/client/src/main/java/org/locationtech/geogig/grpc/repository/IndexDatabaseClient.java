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

import static org.locationtech.geogig.grpc.common.Utils.id;
import static org.locationtech.geogig.grpc.common.Utils.newIndexInfo;
import static org.locationtech.geogig.grpc.common.Utils.newRpcIndexInfo;
import static org.locationtech.geogig.grpc.common.Utils.tuple;
import static org.locationtech.geogig.grpc.repository.ObjectStoreClientBridge.forIndexDatabase;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.grpc.storage.IndexDatabaseGrpc.IndexDatabaseBlockingStub;
import org.locationtech.geogig.grpc.storage.IndexDatabaseGrpc.IndexDatabaseStub;
import org.locationtech.geogig.grpc.storage.IndexedTreeInfo;
import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.repository.Hints;
import org.locationtech.geogig.repository.IndexInfo;
import org.locationtech.geogig.repository.IndexInfo.IndexType;
import org.locationtech.geogig.repository.RepositoryConnectionException;
import org.locationtech.geogig.storage.IndexDatabase;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.protobuf.BoolValue;

import io.grpc.ManagedChannel;

public class IndexDatabaseClient extends ObjectStoreClient implements IndexDatabase {

    private IndexDatabaseBlockingStub blockingStub;

    private IndexDatabaseStub asyncStub;

    @Inject
    public IndexDatabaseClient(Hints hints) {
        super(hints);
    }

    @VisibleForTesting
    public IndexDatabaseClient(ManagedChannel channel, boolean readOnly) {
        super(channel, readOnly);
    }

    @Override
    protected ObjectStoreClientBridge openInternal() {
        this.blockingStub = stubs.newIndexDatabaseBlockingStub();
        this.asyncStub = stubs.newIndexDatabaseStub();
        return forIndexDatabase(blockingStub, asyncStub);
    }

    @Override
    public void close() {
        super.close();
    }

    @Override
    public void configure() throws RepositoryConnectionException {
        // TODO Auto-generated method stub
    }

    @Override
    public boolean isReadOnly() {
        return stubs.isReadOnly();
    }

    @Override
    public boolean checkConfig() throws RepositoryConnectionException {
        // TODO Auto-generated method stub
        return true;
    }

    @Override
    public IndexInfo createIndexInfo(String treeName, String attributeName, IndexType strategy,
            @Nullable Map<String, Object> metadata) {

        IndexInfo info = new IndexInfo(treeName, attributeName, strategy, metadata);
        org.locationtech.geogig.grpc.storage.IndexInfo request = newRpcIndexInfo(info);
        org.locationtech.geogig.grpc.storage.IndexInfo created = blockingStub
                .createIndexInfo(request);

        return newIndexInfo(created);
    }

    @Override
    public IndexInfo updateIndexInfo(String treeName, String attributeName, IndexType strategy,
            Map<String, Object> metadata) {

        IndexInfo info = new IndexInfo(treeName, attributeName, strategy, metadata);
        org.locationtech.geogig.grpc.storage.IndexInfo request = newRpcIndexInfo(info);
        org.locationtech.geogig.grpc.storage.IndexInfo response = blockingStub
                .updateIndexInfo(request);

        return newIndexInfo(response);
    }

    @Override
    public Optional<IndexInfo> getIndexInfo(String treeName, String attributeName) {
        List<IndexInfo> infos = getIndexInfos(treeName, attributeName);
        IndexInfo info = null;
        if (!infos.isEmpty()) {
            Preconditions.checkState(infos.size() == 1);
            info = infos.get(0);
        }
        return Optional.fromNullable(info);
    }

    @Override
    public List<IndexInfo> getIndexInfos(String treeName) {
        return getIndexInfos(treeName, null);
    }

    @Override
    public List<IndexInfo> getIndexInfos() {
        return getIndexInfos(null, null);
    }

    private List<IndexInfo> getIndexInfos(@Nullable String treeName,
            @Nullable String attributeName) {

        Iterator<org.locationtech.geogig.grpc.storage.IndexInfo> infos;
        infos = blockingStub.getIndexInfos(tuple(treeName, attributeName));

        List<IndexInfo> result = new LinkedList<>();
        infos.forEachRemaining((i) -> result.add(newIndexInfo(i)));
        return result;
    }

    @Override
    public boolean dropIndex(IndexInfo index) {

        org.locationtech.geogig.grpc.storage.IndexInfo request = newRpcIndexInfo(index);
        BoolValue response = blockingStub.dropIndexInfo(request);
        return response.getValue();
    }

    @Override
    public void clearIndex(IndexInfo index) {
        org.locationtech.geogig.grpc.storage.IndexInfo request = newRpcIndexInfo(index);
        blockingStub.clearIndex(request);
    }

    @Override
    public void addIndexedTree(IndexInfo index, ObjectId originalTree, ObjectId indexedTree) {
        org.locationtech.geogig.grpc.storage.IndexInfo rpcinfo = newRpcIndexInfo(index);
        IndexedTreeInfo request = IndexedTreeInfo.newBuilder()//
                .setIndexInfo(rpcinfo)//
                .setOriginalTree(id(originalTree))//
                .setIndexedTree(id(indexedTree))//
                .build();
        blockingStub.addIndexedTree(request);
    }

    @Override
    public Optional<ObjectId> resolveIndexedTree(IndexInfo index, ObjectId treeId) {

        org.locationtech.geogig.grpc.storage.IndexInfo rpcinfo = newRpcIndexInfo(index);

        IndexedTreeInfo request = IndexedTreeInfo.newBuilder()//
                .setIndexInfo(rpcinfo)//
                .setOriginalTree(id(treeId))//
                .build();
        org.locationtech.geogig.grpc.model.ObjectId objectId = blockingStub
                .resolveIndexedTree(request);
        @Nullable
        ObjectId result = id(objectId);
        return Optional.fromNullable(result);
    }

}
