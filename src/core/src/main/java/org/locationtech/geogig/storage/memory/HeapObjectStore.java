/* Copyright (c) 2012-2016 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.storage.memory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterators.getNext;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.model.NodeRef;
import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.RevCommit;
import org.locationtech.geogig.model.RevFeature;
import org.locationtech.geogig.model.RevFeatureType;
import org.locationtech.geogig.model.RevObject;
import org.locationtech.geogig.model.RevTag;
import org.locationtech.geogig.model.RevTree;
import org.locationtech.geogig.storage.AbstractStore;
import org.locationtech.geogig.storage.AutoCloseableIterator;
import org.locationtech.geogig.storage.BulkOpListener;
import org.locationtech.geogig.storage.ObjectInfo;
import org.locationtech.geogig.storage.ObjectStore;
import org.locationtech.geogig.storage.impl.AbstractObjectStore;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;

import lombok.NonNull;

/**
 * Provides an implementation of a GeoGig object database that utilizes the heap for the storage of
 * objects.
 * 
 * @see AbstractObjectStore
 */
public class HeapObjectStore extends AbstractStore implements ObjectStore {

    private final ConcurrentMap<ObjectId, RevObject> objects = new ConcurrentHashMap<>();

    public HeapObjectStore() {
        super(false);
    }

    public HeapObjectStore(boolean ro) {
        super(ro);
    }

    /**
     * Determines if the given {@link ObjectId} exists in the object database.
     * 
     * @param id the id to search for
     * @return true if the object exists, false otherwise
     */
    public @Override boolean exists(@NonNull ObjectId id) {
        checkState(isOpen(), "db is closed");
        return objects.containsKey(id);
    }

    private <T extends RevObject> T get(@NonNull ObjectId id, @NonNull Class<T> type,
            boolean failIfAbsent) throws IllegalArgumentException {
        checkState(isOpen(), "db is closed");
        RevObject o = objects.get(id);

        if (null != o && !type.isAssignableFrom(o.getClass())) {
            o = null;
        }
        final boolean fail = o == null && failIfAbsent;

        if (fail) {
            throw new IllegalArgumentException("object does not exist: " + id);
        }
        return o == null ? null : type.cast(o);
    }

    public @Override <T extends RevObject> T get(ObjectId id, Class<T> type)
            throws IllegalArgumentException {
        return get(id, type, true);
    }

    public @Override @Nullable RevObject getIfPresent(ObjectId id) {
        return get(id, RevObject.class, false);
    }

    public @Override @Nullable <T extends RevObject> T getIfPresent(ObjectId id, Class<T> type)
            throws IllegalArgumentException {
        return get(id, type, false);
    }

    /**
     * Deletes the object with the provided {@link ObjectId id} from the database.
     * 
     * @param objectId the id of the object to delete
     */
    public @Override void delete(@NonNull ObjectId objectId) {
        checkState(isOpen(), "db is closed");
        objects.remove(objectId);
    }

    /**
     * Searches the database for {@link ObjectId}s that match the given partial id.
     * 
     * @param partialId the partial id to search for
     * @return a list of matching results
     */
    public @Override List<ObjectId> lookUp(@NonNull String partialId) {
        Preconditions.checkArgument(partialId.length() > 7,
                "partial id must be at least 8 characters long: ", partialId);
        checkState(isOpen(), "db is closed");
        List<ObjectId> matches = Lists.newLinkedList();
        for (ObjectId id : objects.keySet()) {
            if (id.toString().startsWith(partialId)) {
                matches.add(id);
            }
        }
        return matches;
    }

    public @Override boolean put(@NonNull RevObject object) {
        checkArgument(!object.getId().isNull(), "ObjectId is NULL");
        checkState(isOpen(), "db is closed");

        ObjectId id = object.getId();
        RevObject existing = objects.putIfAbsent(id, object);
        return null == existing;
    }

    public @Override void putAll(@NonNull Stream<? extends RevObject> objects,
            @NonNull BulkOpListener listener) {
        checkState(isOpen(), "db is closed");

        objects.forEach(o -> {
            if (put(o)) {
                listener.inserted(o.getId(), null);
            } else {
                listener.found(o.getId(), null);
            }
        });
    }

    public @Override void deleteAll(@NonNull Stream<ObjectId> ids,
            @NonNull BulkOpListener listener) {
        checkState(isOpen(), "db is closed");

        ids.forEach(id -> {
            RevObject removed = this.objects.remove(id);
            if (removed == null) {
                listener.notFound(id);
            } else {
                listener.deleted(id);
            }
        });
    }

    public @Override <T extends RevObject> Stream<T> getAll(@NonNull Stream<ObjectId> ids,
            @NonNull BulkOpListener listener, @NonNull Class<T> type) {
        checkState(isOpen(), "db is closed");

        return ids.map(id -> {
            T obj = getIfPresent(id, type);
            if (null == obj) {
                listener.notFound(id);
            } else {
                listener.found(id, null);
            }
            return obj;
        }).filter(Objects::nonNull);
    }

    public @Override String toString() {
        return getClass().getSimpleName();
    }

    public int size() {
        return this.objects.size();
    }

    public @Override <T extends RevObject> AutoCloseableIterator<ObjectInfo<T>> getObjects(
            @NonNull Iterator<NodeRef> refs, @NonNull BulkOpListener listener,
            @NonNull Class<T> type) {

        checkState(isOpen(), "Database is closed");

        Iterator<ObjectInfo<T>> it = new AbstractIterator<ObjectInfo<T>>() {
            protected @Override ObjectInfo<T> computeNext() {
                checkState(isOpen(), "Database is closed");
                NodeRef ref;
                while ((ref = getNext(refs, null)) != null) {
                    ObjectId id = ref.getObjectId();
                    RevObject obj = getIfPresent(id);
                    if (obj == null || !type.isInstance(obj)) {
                        listener.notFound(id);
                    } else {
                        listener.found(id, null);
                        return ObjectInfo.of(ref, type.cast(obj));
                    }
                }
                return endOfData();
            }
        };

        return AutoCloseableIterator.fromIterator(it);
    }

    public @Override RevTree getTree(ObjectId id) {
        return get(id, RevTree.class, true);
    }

    public @Override RevFeature getFeature(ObjectId id) {
        return get(id, RevFeature.class, true);
    }

    public @Override RevFeatureType getFeatureType(ObjectId id) {
        return get(id, RevFeatureType.class, true);
    }

    public @Override RevCommit getCommit(ObjectId id) {
        return get(id, RevCommit.class, true);
    }

    public @Override RevTag getTag(ObjectId id) {
        return get(id, RevTag.class, true);
    }
}
