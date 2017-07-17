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

import static com.google.common.base.Strings.emptyToNull;
import static org.locationtech.geogig.grpc.common.Utils.newBlob;
import static org.locationtech.geogig.grpc.common.Utils.str;
import static org.locationtech.geogig.grpc.common.Utils.toConflict;
import static org.locationtech.geogig.grpc.common.Utils.toConflictMessage;
import static org.locationtech.geogig.grpc.common.Utils.tuple;
import static org.locationtech.geogig.grpc.repository.ObjectStoreClientBridge.forObjectDatabase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.grpc.client.BasicObserver;
import org.locationtech.geogig.grpc.storage.BlobMessage;
import org.locationtech.geogig.grpc.storage.ConflictMessage;
import org.locationtech.geogig.grpc.storage.ObjectDatabaseGrpc.ObjectDatabaseBlockingStub;
import org.locationtech.geogig.grpc.storage.ObjectDatabaseGrpc.ObjectDatabaseStub;
import org.locationtech.geogig.grpc.storage.ScopedPaths;
import org.locationtech.geogig.repository.Conflict;
import org.locationtech.geogig.repository.Hints;
import org.locationtech.geogig.repository.RepositoryConnectionException;
import org.locationtech.geogig.storage.BlobStore;
import org.locationtech.geogig.storage.ConflictsDatabase;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.locationtech.geogig.storage.impl.TransactionBlobStore;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import com.google.protobuf.BoolValue;
import com.google.protobuf.Empty;
import com.google.protobuf.StringValue;
import com.google.protobuf.UInt64Value;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

public class ObjectDatabaseClient extends ObjectStoreClient implements ObjectDatabase {

    private ObjectDatabaseBlockingStub blockingStub;

    private ObjectDatabaseStub asyncStub;

    private TransactionBlobStore blobStore;

    private ConflictsDatabase conflicts;

    @Inject
    public ObjectDatabaseClient(Hints hints) {
        super(hints);
    }

    @VisibleForTesting
    public ObjectDatabaseClient(ManagedChannel channel, boolean readOnly) {
        super(channel, readOnly);
    }

    @Override
    public void close() {
        super.close();
        blockingStub = null;
        asyncStub = null;
        blobStore = null;
        conflicts = null;
    }

    @Override
    protected ObjectStoreClientBridge openInternal() {
        this.blockingStub = stubs.newObjectDatabaseBlockingStub();
        this.asyncStub = stubs.newObjectDatabaseStub();
        this.blobStore = new BlobStoreClient();
        this.conflicts = new ConflictsClient();
        return forObjectDatabase(blockingStub, asyncStub);
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
        return true;
    }

    @Override
    public ConflictsDatabase getConflictsDatabase() {
        return conflicts;
    }

    @Override
    public BlobStore getBlobStore() {
        return blobStore;
    }

    private class BlobStoreClient implements TransactionBlobStore {

        private final String NO_TX = "";

        @Override
        public Optional<byte[]> getBlob(String path) {
            return getBlob(NO_TX, path);
        }

        @Override
        public Optional<byte[]> getBlob(String namespace, String path) {
            byte[] val = get(namespace, path);
            return Optional.fromNullable(val);
        }

        @Override
        public Optional<InputStream> getBlobAsStream(String path) {
            return getBlobAsStream(NO_TX, path);
        }

        @Override
        public Optional<InputStream> getBlobAsStream(String namespace, String path) {
            byte[] val = get(namespace, path);
            InputStream is = val == null ? null : new ByteArrayInputStream(val);
            return Optional.fromNullable(is);
        }

        @Override
        public void putBlob(String path, InputStream blob) {
            putBlob(NO_TX, path, blob);
        }

        @Override
        public void putBlob(String path, byte[] blob) {
            putBlob(NO_TX, path, blob);
        }

        @Override
        public void putBlob(String namespace, String path, InputStream blob) {
            try {
                putBlob(namespace, path, ByteStreams.toByteArray(blob));
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void removeBlob(String path) {
            removeBlob(NO_TX, path);
        }

        private @Nullable byte[] get(String namespace, String path) {
            Preconditions.checkNotNull(namespace, "namespace can't be null");
            Preconditions.checkNotNull(path, "path can't be null");
            namespace = emptyToNull(namespace);
            BlobMessage blob = blockingStub.getBlob(newBlob(namespace, path, null));
            byte[] array = null;
            if (blob.getFound()) {
                array = blob.getContent().toByteArray();
            }
            return array;
        }

        @Override
        public void putBlob(String namespace, String path, byte[] blob) {
            Preconditions.checkNotNull(namespace, "namespace can't be null");
            Preconditions.checkNotNull(path, "path can't be null");

            BlobMessage message = newBlob(emptyToNull(namespace), path, blob);
            blockingStub.putBlob(message);
        }

        @Override
        public void removeBlob(String namespace, String path) {
            Preconditions.checkNotNull(namespace, "namespace can't be null");
            Preconditions.checkNotNull(path, "path can't be null");

            BlobMessage message = newBlob(emptyToNull(namespace), path, null);
            blockingStub.removeBlob(message);
        }

        @Override
        public void removeBlobs(String namespace) {
            namespace = emptyToNull(namespace);
            Preconditions.checkNotNull(namespace, "namespace can't be null");

            blockingStub.removeBlobs(str(namespace));
        }
    }

    private class ConflictsClient implements ConflictsDatabase {

        @Override
        public boolean hasConflicts(@Nullable String namespace) {
            BoolValue res = blockingStub.hasConflicts(str(namespace));
            boolean value = res.getValue();
            return value;
        }

        @Override
        public Optional<Conflict> getConflict(@Nullable String namespace, String path) {
            org.locationtech.geogig.grpc.storage.Conflict rpcconflict;
            rpcconflict = blockingStub.getConflict(tuple(namespace, path));
            Conflict conflict = toConflict(rpcconflict);
            return Optional.fromNullable(conflict);
        }

        @Override
        public List<Conflict> getConflicts(@Nullable String namespace,
                @Nullable String pathFilter) {

            Iterator<Conflict> iterator = getByPrefix(namespace, pathFilter);
            return Lists.newArrayList(iterator);
        }

        @Override
        public Iterator<Conflict> getByPrefix(@Nullable String namespace,
                @Nullable String prefixFilter) {

            List<Conflict> conflicts = new ArrayList<>();

            BasicObserver<org.locationtech.geogig.grpc.storage.Conflict> responseObserver;
            responseObserver = new BasicObserver<org.locationtech.geogig.grpc.storage.Conflict>() {
                @Override
                public void onNext(org.locationtech.geogig.grpc.storage.Conflict value) {
                    conflicts.add(toConflict(value));
                }
            };
            asyncStub.getConflicts(tuple(namespace, prefixFilter), responseObserver);
            while (!responseObserver.isCompleted()) {
                Thread.yield();
            }
            responseObserver.rethrow();
            return conflicts.iterator();
        }

        @Override
        public long getCountByPrefix(@Nullable String namespace, @Nullable String treePath) {
            UInt64Value res = blockingStub.getConflictsCount(tuple(namespace, treePath));
            long count = res.getValue();
            return count;
        }

        @Override
        public void addConflict(@Nullable String namespace, Conflict conflict) {
            addConflicts(namespace, Collections.singleton(conflict));
        }

        @Override
        public void addConflicts(@Nullable String namespace, Iterable<Conflict> conflicts) {
            BasicObserver<Empty> responseObserver = BasicObserver.newEmptyObserver();
            StreamObserver<ConflictMessage> uploadObserver;

            uploadObserver = asyncStub.addConflicts(responseObserver);

            try {
                conflicts.forEach((c) -> uploadObserver.onNext(toConflictMessage(namespace, c)));
                uploadObserver.onCompleted();
            } catch (Exception e) {
                uploadObserver.onError(e);
            }
            while (!responseObserver.isCompleted()) {
                Thread.yield();
            }
            responseObserver.rethrow();
        }

        @Override
        public void removeConflict(@Nullable String namespace, String path) {
            removeConflicts(namespace, Collections.singleton(path));
        }

        @Override
        public void removeConflicts(@Nullable String namespace) {
            removeByPrefix(namespace, null);
        }

        @Override
        public void removeConflicts(@Nullable String namespace, Iterable<String> paths) {
            ScopedPaths.Builder builder = ScopedPaths.newBuilder();
            if (null != namespace) {
                builder.setNamespace(namespace);
            }
            paths.forEach((p) -> builder.addPaths(p));
            ScopedPaths request = builder.build();
            blockingStub.removeConflicts(request);
        }

        @Override
        public Set<String> findConflicts(@Nullable String namespace, Set<String> paths) {
            ScopedPaths.Builder builder = ScopedPaths.newBuilder();
            if (null != namespace) {
                builder.setNamespace(namespace);
            }
            paths.forEach((p) -> builder.addPaths(p));
            ScopedPaths request = builder.build();
            Iterator<StringValue> results = blockingStub.findConflicts(request);
            Set<String> ret = Sets.newHashSet(Iterators.transform(results, (s) -> s.getValue()));
            return ret;
        }

        @Override
        public void removeByPrefix(@Nullable String namespace, @Nullable String pathPrefix) {

            blockingStub.removeConflictsByPrefix(tuple(namespace, pathPrefix));
        }

    }
}
