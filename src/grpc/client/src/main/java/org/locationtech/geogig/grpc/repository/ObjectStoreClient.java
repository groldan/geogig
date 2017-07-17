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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.locationtech.geogig.grpc.common.Utils.id;
import static org.locationtech.geogig.grpc.common.Utils.messageToObject;
import static org.locationtech.geogig.grpc.common.Utils.object;
import static org.locationtech.geogig.grpc.common.Utils.queryObject;
import static org.locationtech.geogig.grpc.common.Utils.str;
import static org.locationtech.geogig.grpc.repository.ObjectStoreClientBridge.forPlainStore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.grpc.client.BasicObserver;
import org.locationtech.geogig.grpc.client.Stubs;
import org.locationtech.geogig.grpc.common.BlockingQueueIterator;
import org.locationtech.geogig.grpc.storage.BatchResult;
import org.locationtech.geogig.grpc.storage.BatchResult.Status;
import org.locationtech.geogig.grpc.storage.ObjectStoreGrpc;
import org.locationtech.geogig.grpc.storage.ObjectStoreGrpc.ObjectStoreBlockingStub;
import org.locationtech.geogig.grpc.storage.ObjectStoreGrpc.ObjectStoreStub;
import org.locationtech.geogig.model.NodeRef;
import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.RevCommit;
import org.locationtech.geogig.model.RevFeature;
import org.locationtech.geogig.model.RevFeatureType;
import org.locationtech.geogig.model.RevObject;
import org.locationtech.geogig.model.RevTag;
import org.locationtech.geogig.model.RevTree;
import org.locationtech.geogig.repository.Hints;
import org.locationtech.geogig.storage.AutoCloseableIterator;
import org.locationtech.geogig.storage.BulkOpListener;
import org.locationtech.geogig.storage.BulkOpListener.CountingListener;
import org.locationtech.geogig.storage.ObjectInfo;
import org.locationtech.geogig.storage.ObjectStore;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;

import io.grpc.ManagedChannel;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;

public class ObjectStoreClient implements ObjectStore {

    protected ObjectStoreClientBridge bridge;

    protected final Stubs stubs;

    private static final ExecutorService asyncResponseExecutor = Executors.newCachedThreadPool();

    protected ObjectStoreClient(Hints hints) {
        this.stubs = Stubs.create(hints);
    }

    @VisibleForTesting
    public ObjectStoreClient(ManagedChannel channel, boolean readOnly) {
        this.stubs = Stubs.create(channel, readOnly);
    }

    @Override
    public final void open() {
        if (stubs.open()) {
            bridge = openInternal();
        }
    }

    // subclasses shall override if they need to do something else during open
    protected ObjectStoreClientBridge openInternal() {
        ObjectStoreBlockingStub blockingStub = ObjectStoreGrpc.newBlockingStub(stubs.channel());
        ObjectStoreStub asyncStub = ObjectStoreGrpc.newStub(stubs.channel());
        return forPlainStore(blockingStub, asyncStub);
    }

    @Override
    public void close() {
        stubs.close();
        bridge = null;
    }

    @Override
    public boolean isOpen() {
        return stubs.isOpen();
    }

    @Override
    public boolean exists(ObjectId id) {
        checkNotNull(id, "argument id is null");
        stubs.checkOpen();
        boolean exists = bridge.exists(id(id)).getValue();
        return exists;
    }

    @Override
    public List<ObjectId> lookUp(String partialId) {
        checkNotNull(partialId, "argument partialId is null");
        checkArgument(partialId.length() > 7, "partial id must be at least 8 characters long: ",
                partialId);
        stubs.checkOpen();

        Iterator<org.locationtech.geogig.grpc.model.ObjectId> matches = bridge
                .lookUp(str(partialId));

        List<ObjectId> result = new ArrayList<>();
        matches.forEachRemaining((id) -> result.add(id(id)));

        return result;
    }

    @Override
    public RevObject get(ObjectId id) throws IllegalArgumentException {
        return get(id, RevObject.class);
    }

    @Override
    public <T extends RevObject> T get(ObjectId id, Class<T> type) throws IllegalArgumentException {
        @Nullable
        T result = getIfPresent(id, type);

        if (result == null) {
            throw new IllegalArgumentException(id + " does not exist");
        }
        return result;
    }

    @Override
    public @Nullable RevObject getIfPresent(ObjectId id) {
        return getIfPresent(id, RevObject.class);
    }

    @Override
    public @Nullable <T extends RevObject> T getIfPresent(ObjectId id, Class<T> type)
            throws IllegalArgumentException {

        checkNotNull(id, "argument id is null");
        checkNotNull(type, "argument class is null");
        stubs.checkOpen();

        Iterator<T> iterator;
        iterator = getAll(Collections.singleton(id), BulkOpListener.NOOP_LISTENER, type);

        T result = null;
        if (iterator.hasNext()) {
            result = iterator.next();
        }
        return result;
    }

    @Override
    public RevTree getTree(ObjectId id) {
        return get(id, RevTree.class);
    }

    @Override
    public RevFeature getFeature(ObjectId id) {
        return get(id, RevFeature.class);
    }

    @Override
    public RevFeatureType getFeatureType(ObjectId id) {
        return get(id, RevFeatureType.class);
    }

    @Override
    public RevCommit getCommit(ObjectId id) {
        return get(id, RevCommit.class);
    }

    @Override
    public RevTag getTag(ObjectId id) {
        return get(id, RevTag.class);
    }

    @Override
    public boolean put(RevObject object) {
        checkNotNull(object, "argument object is null");
        checkArgument(!object.getId().isNull(), "ObjectId is NULL %s", object);
        stubs.checkWritable();

        CountingListener listener = BulkOpListener.newCountingListener();
        putAll(Iterators.singletonIterator(object), listener);

        int inserted = listener.inserted();

        return inserted > 0;
    }

    @Override
    public void delete(ObjectId objectId) {
        checkNotNull(objectId, "argument objectId is null");
        deleteAll(Iterators.singletonIterator(objectId));
    }

    @Override
    public Iterator<RevObject> getAll(Iterable<ObjectId> ids) {
        return getAll(ids, BulkOpListener.NOOP_LISTENER);
    }

    @Override
    public Iterator<RevObject> getAll(Iterable<ObjectId> ids, BulkOpListener listener) {
        return getAll(ids, listener, RevObject.class);
    }

    @Override
    public <T extends RevObject> Iterator<T> getAll(Iterable<ObjectId> ids, BulkOpListener listener,
            Class<T> type) {

        checkNotNull(ids, "ids is null");
        checkNotNull(listener, "listener is null");
        checkNotNull(type, "type is null");
        stubs.checkOpen();

        BasicObserver<org.locationtech.geogig.grpc.model.RevObject> responseObserver;

        BlockingQueueIterator<T> iterator = new BlockingQueueIterator<>();

        responseObserver = new BasicObserver<org.locationtech.geogig.grpc.model.RevObject>() {

            @Override
            public void onNext(org.locationtech.geogig.grpc.model.RevObject value) {
                RevObject obj = object(value);
                if (obj == null || !type.isInstance(obj)) {
                    ObjectId id = id(value.getId());
                    listener.notFound(id);
                } else {
                    iterator.offer(type.cast(obj));
                    listener.found(obj.getId(), null);
                }
            }

            @Override
            public void onCompleted() {
                super.onCompleted();
                iterator.completed();
            }

            @Override
            public void onError(Throwable t) {
                super.onError(t);
                iterator.abort(t);
            }
        };

        StreamObserver<org.locationtech.geogig.grpc.model.ObjectId> uploadStream;
        uploadStream = bridge.get(responseObserver);

        asyncResponseExecutor.submit(() -> {
            for (ObjectId id : ids) {
                if (!responseObserver.isErrored()) {
                    uploadStream.onNext(id(id));
                }
            }
            uploadStream.onCompleted();
        });

        return iterator;
    }

    @Override
    public void putAll(Iterator<? extends RevObject> objects) {
        putAll(objects, BulkOpListener.NOOP_LISTENER);
    }

    private static class RemoteListenerStream extends BasicObserver<BatchResult> {
        private BulkOpListener listener;

        public RemoteListenerStream(BulkOpListener listener) {
            this.listener = listener;
        }

        @Override
        public void onNext(BatchResult value) {
            Status status = value.getStatus();
            ObjectId id = id(value.getId());
            // System.err.println("onNext: " + status + ", " + id + ", " +
            // Thread.currentThread().getName());
            switch (status) {
            case INSERTED:
                listener.inserted(id, null);
                break;
            case FOUND:
                listener.found(id, null);
                break;
            case DELETED:
                listener.deleted(id);
                break;
            case NOT_FOUND:
                listener.notFound(id);
                break;
            default:
                throw new IllegalStateException(
                        "Invalid status for this operation: " + status + " id: " + id);
            }

        }
    }

    @Override
    public void putAll(Iterator<? extends RevObject> objects, BulkOpListener listener) {
        checkNotNull(objects, "objects is null");
        checkNotNull(listener, "listener is null");
        stubs.checkWritable();

        RemoteListenerStream listenerStream = new RemoteListenerStream(listener);

        StreamObserver<org.locationtech.geogig.grpc.model.RevObject> uploadStream;

        uploadStream = bridge.put(listenerStream);

        while (objects.hasNext()) {
            RevObject o = objects.next();
            org.locationtech.geogig.grpc.model.RevObject next = object(o);
            uploadStream.onNext(next);
        }
        uploadStream.onCompleted();

        listenerStream.awaitCompleteness();
    }

    @Override
    public void deleteAll(Iterator<ObjectId> ids) {
        deleteAll(ids, BulkOpListener.NOOP_LISTENER);
    }

    @Override
    public void deleteAll(Iterator<ObjectId> ids, BulkOpListener listener) {
        checkNotNull(ids, "argument objectId is null");
        checkNotNull(listener, "argument listener is null");
        stubs.checkWritable();

        RemoteListenerStream listenerStream = new RemoteListenerStream(listener);

        StreamObserver<org.locationtech.geogig.grpc.model.ObjectId> uploadStream;

        uploadStream = bridge.delete(listenerStream);

        while (ids.hasNext()) {
            ObjectId id = ids.next();
            org.locationtech.geogig.grpc.model.ObjectId next = id(id);
            uploadStream.onNext(next);
        }
        uploadStream.onCompleted();

        listenerStream.awaitCompleteness();
    }

    @Override
    public <T extends RevObject> AutoCloseableIterator<ObjectInfo<T>> getObjects(
            Iterator<NodeRef> nodes, BulkOpListener listener, Class<T> type) {
        checkNotNull(nodes, "refs is null");
        checkNotNull(listener, "listener is null");
        checkNotNull(type, "type is null");
        stubs.checkOpen();

        List<ObjectInfo<T>> result = new ArrayList<>();

        BasicObserver<org.locationtech.geogig.grpc.storage.ObjectInfo> responseObserver = new BasicObserver<org.locationtech.geogig.grpc.storage.ObjectInfo>() {

            @Override
            public void onNext(org.locationtech.geogig.grpc.storage.ObjectInfo value) {
                org.locationtech.geogig.grpc.model.RevObject object = value.getObject();
                if (object.getSerialForm().isEmpty()) {
                    ObjectId id = id(object.getId());
                    listener.notFound(id);
                } else {
                    ObjectInfo<RevObject> objectInfo = messageToObject(value);
                    RevObject revObject = objectInfo.object();
                    if (type.isInstance(revObject)) {
                        result.add((ObjectInfo<T>) objectInfo);
                        listener.found(revObject.getId(), null);
                    } else {
                        listener.notFound(revObject.getId());
                    }
                }
            }
        };

        ClientCallStreamObserver<org.locationtech.geogig.grpc.storage.ObjectInfo> uploadStream;

        uploadStream = (ClientCallStreamObserver<org.locationtech.geogig.grpc.storage.ObjectInfo>) bridge
                .getObjects(responseObserver);

        // uploadStream.setOnReadyHandler(() -> {
        // while (uploadStream.isReady() && nodes.hasNext()) {
        // NodeRef node = nodes.next();
        // org.locationtech.geogig.grpc.storage.ObjectInfo queryObject = queryObject(node);
        // uploadStream.onNext(queryObject);
        // }
        // if (!nodes.hasNext()) {
        // uploadStream.onCompleted();
        // }
        // });

        while (nodes.hasNext()) {
            NodeRef node = nodes.next();
            org.locationtech.geogig.grpc.storage.ObjectInfo queryObject = queryObject(node);
            while (!uploadStream.isReady()) {
//                Thread.yield();
            }
            uploadStream.onNext(queryObject);
        }
        uploadStream.onCompleted();

        responseObserver.awaitCompleteness();

        return AutoCloseableIterator.fromIterator(result.iterator());
    }
}
