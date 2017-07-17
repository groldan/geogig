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
import static org.locationtech.geogig.grpc.common.Utils.id;
import static org.locationtech.geogig.grpc.common.Utils.object;
import static org.locationtech.geogig.grpc.common.Utils.objectToMessage;
import static org.locationtech.geogig.grpc.common.Utils.queryObjectToRef;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.grpc.storage.BatchResult;
import org.locationtech.geogig.grpc.storage.BatchResult.Status;
import org.locationtech.geogig.grpc.storage.ObjectStoreGrpc;
import org.locationtech.geogig.model.NodeRef;
import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.RevObject;
import org.locationtech.geogig.storage.AutoCloseableIterator;
import org.locationtech.geogig.storage.BulkOpListener;
import org.locationtech.geogig.storage.ObjectInfo;
import org.locationtech.geogig.storage.ObjectStore;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.protobuf.BoolValue;
import com.google.protobuf.StringValue;

import io.grpc.stub.StreamObserver;

public class ObjectStoreService extends ObjectStoreGrpc.ObjectStoreImplBase {

    protected final Supplier<? extends ObjectStore> _localdb;

    private static final ExecutorService asyncResponseExecutor = Executors.newCachedThreadPool();

    @VisibleForTesting
    public ObjectStoreService(ObjectStore localdb) {
        this(Suppliers.ofInstance(localdb));
    }

    public ObjectStoreService(Supplier<? extends ObjectStore> localdb) {
        checkNotNull(localdb);
        this._localdb = localdb;
    }

    private ObjectStore localdb() {
        ObjectStore store = _localdb.get();
        checkNotNull(store);
        return store;
    }

    @Override
    public void exists(org.locationtech.geogig.grpc.model.ObjectId request,
            StreamObserver<BoolValue> responseObserver) {
        try {
            ObjectId id = id(request);
            boolean exists = localdb().exists(id);
            responseObserver.onNext(BoolValue.newBuilder().setValue(exists).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void lookUp(StringValue request,
            StreamObserver<org.locationtech.geogig.grpc.model.ObjectId> response) {

        String partialId = request.getValue();
        try {
            List<ObjectId> res = localdb().lookUp(partialId);
            res.forEach((id) -> response.onNext(id(id)));
            response.onCompleted();
        } catch (RuntimeException e) {
            response.onError(e);
        }
    }

    @Override
    public StreamObserver<org.locationtech.geogig.grpc.model.RevObject> put(
            StreamObserver<BatchResult> responseObserver) {

        final ObjectStore store = localdb();

        BulkOpListener listener = new BulkOpListener() {
            public void found(ObjectId id, @Nullable Integer storageSizeBytes) {
                onNext(id, BatchResult.Status.FOUND);
            }

            public void inserted(ObjectId id, @Nullable Integer storageSizeBytes) {
                onNext(id, BatchResult.Status.INSERTED);
            }

            private void onNext(ObjectId id, Status status) {
                BatchResult notification = batchResult(id, status);
                responseObserver.onNext(notification);
            }

            public void deleted(ObjectId id) {
                throw new IllegalStateException("call to method deleted not expected here");
            }

            public void notFound(ObjectId id) {
                throw new IllegalStateException("call to method notFound not expected here");
            }

        };

        StreamObserver<org.locationtech.geogig.grpc.model.RevObject> incoming = new StreamObserver<org.locationtech.geogig.grpc.model.RevObject>() {

            final int batchSize = 1000;

            final List<RevObject> batch = new ArrayList<>(batchSize);

            @Override
            public void onNext(org.locationtech.geogig.grpc.model.RevObject value) {
                RevObject revObject = object(value);
                Preconditions.checkNotNull(revObject);
                batch.add(revObject);
                if (batchSize == batch.size()) {
                    flush();
                }
            }

            @Override
            public void onError(Throwable t) {
                // queue.abort(t);
            }

            @Override
            public void onCompleted() {
                flush();
                responseObserver.onCompleted();
            }

            private void flush() {
                store.putAll(batch.iterator(), listener);
                batch.clear();
            }
        };

        return incoming;
    }

    @Override
    public StreamObserver<org.locationtech.geogig.grpc.model.ObjectId> get(
            StreamObserver<org.locationtech.geogig.grpc.model.RevObject> responseObserver) {

        return new StreamObserver<org.locationtech.geogig.grpc.model.ObjectId>() {
            final ObjectStore store = localdb();

            final List<ObjectId> ids = new ArrayList<>();

            final int buffSize = 1000;

            @Override
            public void onNext(org.locationtech.geogig.grpc.model.ObjectId value) {
                ids.add(id(value));
                if (ids.size() == buffSize) {
                    flush();
                }
            }

            @Override
            public void onError(Throwable t) {
                responseObserver.onError(t);
            }

            @Override
            public void onCompleted() {
                if (!ids.isEmpty()) {
                    flush();
                }
                responseObserver.onCompleted();
            }

            private void flush() {
                try {
                    BulkOpListener listener = new BulkOpListener() {
                        @Override
                        public void notFound(ObjectId id) {
                            org.locationtech.geogig.grpc.model.RevObject missing = object(id, null);
                            responseObserver.onNext(missing);
                        }
                    };
                    Iterator<RevObject> all = store.getAll(ids, listener);
                    all.forEachRemaining((o) -> responseObserver.onNext(object(o)));
                    ids.clear();
                } catch (Exception e) {
                    e.printStackTrace();
                    responseObserver.onError(e);
                }
            }
        };
    }

    @Override
    public StreamObserver<org.locationtech.geogig.grpc.storage.ObjectInfo> getObjects(
            StreamObserver<org.locationtech.geogig.grpc.storage.ObjectInfo> responseObserver) {

        return new StreamObserver<org.locationtech.geogig.grpc.storage.ObjectInfo>() {
            final ObjectStore store = localdb();

            final List<NodeRef> nodes = new ArrayList<>();

            final int buffSize = 10_000;

            @Override
            public void onNext(org.locationtech.geogig.grpc.storage.ObjectInfo queryObject) {
                NodeRef ref = queryObjectToRef(queryObject);
                nodes.add(ref);
                if (nodes.size() == buffSize) {
                    flush();
                }
            }

            @Override
            public void onError(Throwable t) {
                responseObserver.onError(t);
            }

            @Override
            public void onCompleted() {
                if (!nodes.isEmpty()) {
                    flush();
                }
                responseObserver.onCompleted();
            }

            private void flush() {
                BulkOpListener listener = new BulkOpListener() {
                    @Override
                    public void notFound(ObjectId id) {
                        org.locationtech.geogig.grpc.model.RevObject missingObject;
                        org.locationtech.geogig.grpc.storage.ObjectInfo missingResponse;

                        missingObject = object(id, null);
                        missingResponse = org.locationtech.geogig.grpc.storage.ObjectInfo
                                .newBuilder().setObject(missingObject).build();
                        responseObserver.onNext(missingResponse);
                    }
                };

                int size = nodes.size();
                if (size > 32) {
                    System.err.printf("Sending %,d batched objects\n", size);
                }
                try (AutoCloseableIterator<ObjectInfo<RevObject>> result = store
                        .getObjects(nodes.iterator(), listener, RevObject.class)) {
                    result.forEachRemaining((o) -> responseObserver.onNext(objectToMessage(o)));
                }finally{
                    nodes.clear();
                }
            }
        };
    }

    @Override
    public io.grpc.stub.StreamObserver<org.locationtech.geogig.grpc.model.ObjectId> delete(
            io.grpc.stub.StreamObserver<org.locationtech.geogig.grpc.storage.BatchResult> responseObserver) {

        List<ObjectId> ids = new ArrayList<>();
        return new StreamObserver<org.locationtech.geogig.grpc.model.ObjectId>() {

            @Override
            public void onNext(org.locationtech.geogig.grpc.model.ObjectId value) {
                ids.add(id(value));
            }

            @Override
            public void onError(Throwable t) {
                responseObserver.onError(t);
            }

            @Override
            public void onCompleted() {
                BulkOpListener listener = new BulkOpListener() {
                    @Override
                    public void notFound(ObjectId id) {
                        responseObserver.onNext(batchResult(id, Status.NOT_FOUND));
                    }

                    @Override
                    public void deleted(ObjectId id) {
                        responseObserver.onNext(batchResult(id, Status.DELETED));
                    }
                };
                localdb().deleteAll(ids.iterator(), listener);
                responseObserver.onCompleted();
            }
        };
    }

    private static BatchResult batchResult(ObjectId id, Status status) {
        org.locationtech.geogig.grpc.model.ObjectId rpcId = id(id);
        BatchResult notification = BatchResult.newBuilder().setId(rpcId).setStatus(status).build();
        return notification;
    }

}
