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

import java.util.Iterator;

import org.locationtech.geogig.grpc.model.ObjectId;
import org.locationtech.geogig.grpc.model.RevObject;
import org.locationtech.geogig.grpc.storage.BatchResult;
import org.locationtech.geogig.grpc.storage.IndexDatabaseGrpc.IndexDatabaseBlockingStub;
import org.locationtech.geogig.grpc.storage.IndexDatabaseGrpc.IndexDatabaseStub;
import org.locationtech.geogig.grpc.storage.ObjectDatabaseGrpc;
import org.locationtech.geogig.grpc.storage.ObjectDatabaseGrpc.ObjectDatabaseBlockingStub;
import org.locationtech.geogig.grpc.storage.ObjectDatabaseGrpc.ObjectDatabaseStub;
import org.locationtech.geogig.grpc.storage.ObjectInfo;
import org.locationtech.geogig.grpc.storage.ObjectStoreGrpc.ObjectStoreBlockingStub;
import org.locationtech.geogig.grpc.storage.ObjectStoreGrpc.ObjectStoreStub;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.BoolValue;
import com.google.protobuf.StringValue;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

public abstract class ObjectStoreClientBridge {

    public abstract BoolValue exists(org.locationtech.geogig.grpc.model.ObjectId request);

    public abstract java.util.Iterator<org.locationtech.geogig.grpc.model.ObjectId> lookUp(
            StringValue request);

    public abstract io.grpc.stub.StreamObserver<org.locationtech.geogig.grpc.model.RevObject> put(
            io.grpc.stub.StreamObserver<org.locationtech.geogig.grpc.storage.BatchResult> responseObserver);

    public abstract io.grpc.stub.StreamObserver<org.locationtech.geogig.grpc.model.ObjectId> get(
            io.grpc.stub.StreamObserver<org.locationtech.geogig.grpc.model.RevObject> responseObserver);

    public abstract io.grpc.stub.StreamObserver<org.locationtech.geogig.grpc.model.ObjectId> delete(
            io.grpc.stub.StreamObserver<org.locationtech.geogig.grpc.storage.BatchResult> responseObserver);

    public abstract io.grpc.stub.StreamObserver<org.locationtech.geogig.grpc.storage.ObjectInfo> getObjects(
            io.grpc.stub.StreamObserver<org.locationtech.geogig.grpc.storage.ObjectInfo> responseObserver);

    @VisibleForTesting
    public static ObjectStoreClientBridge forPlainStore(ObjectStoreBlockingStub blockingStub,
            ObjectStoreStub asyncStub) {
        return new ObjectStoreBridge(blockingStub, asyncStub);
    }

    public static ObjectStoreClientBridge forObjectDatabase(ManagedChannel channel) {
        ObjectDatabaseBlockingStub blockingStub = ObjectDatabaseGrpc.newBlockingStub(channel);
        ObjectDatabaseStub asyncStub = ObjectDatabaseGrpc.newStub(channel);
        return forObjectDatabase(blockingStub, asyncStub);
    }

    public static ObjectStoreClientBridge forObjectDatabase(ObjectDatabaseBlockingStub blockingStub,
            ObjectDatabaseStub asyncStub) {
        return new ObjectDatabaseBridge(blockingStub, asyncStub);
    }

    public static ObjectStoreClientBridge forIndexDatabase(ManagedChannel channel) {
        ObjectDatabaseBlockingStub blockingStub = ObjectDatabaseGrpc.newBlockingStub(channel);
        ObjectDatabaseStub asyncStub = ObjectDatabaseGrpc.newStub(channel);
        return forObjectDatabase(blockingStub, asyncStub);
    }

    public static ObjectStoreClientBridge forIndexDatabase(IndexDatabaseBlockingStub blockingStub,
            IndexDatabaseStub asyncStub) {
        return new IndexDatabaseBridge(blockingStub, asyncStub);
    }

    private static class ObjectDatabaseBridge extends ObjectStoreClientBridge {

        private ObjectDatabaseBlockingStub blockingStub;

        private ObjectDatabaseStub asyncStub;

        public ObjectDatabaseBridge(ObjectDatabaseBlockingStub blockingStub,
                ObjectDatabaseStub asyncStub) {
            this.blockingStub = blockingStub;
            this.asyncStub = asyncStub;
        }

        @Override
        public BoolValue exists(ObjectId request) {
            return blockingStub.exists(request);
        }

        @Override
        public Iterator<ObjectId> lookUp(StringValue request) {
            return blockingStub.lookUp(request);
        }

        @Override
        public StreamObserver<RevObject> put(StreamObserver<BatchResult> responseObserver) {
            return asyncStub.put(responseObserver);
        }

        @Override
        public StreamObserver<ObjectId> get(StreamObserver<RevObject> responseObserver) {
            return asyncStub.get(responseObserver);
        }

        @Override
        public StreamObserver<ObjectId> delete(StreamObserver<BatchResult> responseObserver) {
            return asyncStub.delete(responseObserver);
        }

        @Override
        public StreamObserver<ObjectInfo> getObjects(StreamObserver<ObjectInfo> responseObserver) {
            return asyncStub.getObjects(responseObserver);
        }
    }

    private static class IndexDatabaseBridge extends ObjectStoreClientBridge {

        private IndexDatabaseBlockingStub blockingStub;

        private IndexDatabaseStub asyncStub;

        public IndexDatabaseBridge(IndexDatabaseBlockingStub blockingStub,
                IndexDatabaseStub asyncStub) {
            this.blockingStub = blockingStub;
            this.asyncStub = asyncStub;
        }

        @Override
        public BoolValue exists(ObjectId request) {
            return blockingStub.exists(request);
        }

        @Override
        public Iterator<ObjectId> lookUp(StringValue request) {
            return blockingStub.lookUp(request);
        }

        @Override
        public StreamObserver<RevObject> put(StreamObserver<BatchResult> responseObserver) {
            return asyncStub.put(responseObserver);
        }

        @Override
        public StreamObserver<ObjectId> get(StreamObserver<RevObject> responseObserver) {
            return asyncStub.get(responseObserver);
        }

        @Override
        public StreamObserver<ObjectId> delete(StreamObserver<BatchResult> responseObserver) {
            return asyncStub.delete(responseObserver);
        }

        @Override
        public StreamObserver<ObjectInfo> getObjects(StreamObserver<ObjectInfo> responseObserver) {
            return asyncStub.getObjects(responseObserver);
        }
    }

    private static class ObjectStoreBridge extends ObjectStoreClientBridge {

        private ObjectStoreBlockingStub blockingStub;

        private ObjectStoreStub asyncStub;

        public ObjectStoreBridge(ObjectStoreBlockingStub blockingStub, ObjectStoreStub asyncStub) {
            this.blockingStub = blockingStub;
            this.asyncStub = asyncStub;
        }

        @Override
        public BoolValue exists(ObjectId request) {
            return blockingStub.exists(request);
        }

        @Override
        public Iterator<ObjectId> lookUp(StringValue request) {
            return blockingStub.lookUp(request);
        }

        @Override
        public StreamObserver<RevObject> put(StreamObserver<BatchResult> responseObserver) {
            return asyncStub.put(responseObserver);
        }

        @Override
        public StreamObserver<ObjectId> get(StreamObserver<RevObject> responseObserver) {
            return asyncStub.get(responseObserver);
        }

        @Override
        public StreamObserver<ObjectId> delete(StreamObserver<BatchResult> responseObserver) {
            return asyncStub.delete(responseObserver);
        }

        @Override
        public StreamObserver<ObjectInfo> getObjects(StreamObserver<ObjectInfo> responseObserver) {
            return asyncStub.getObjects(responseObserver);
        }
    }
}
