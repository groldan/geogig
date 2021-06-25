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

import static com.google.common.base.Preconditions.checkState;

import java.util.stream.Stream;

import org.locationtech.geogig.model.RevCommit;
import org.locationtech.geogig.model.RevObject;
import org.locationtech.geogig.model.RevObject.TYPE;
import org.locationtech.geogig.repository.Hints;
import org.locationtech.geogig.storage.BlobStore;
import org.locationtech.geogig.storage.BulkOpListener;
import org.locationtech.geogig.storage.GraphDatabase;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.locationtech.geogig.storage.decorator.ForwardingObjectStore;

import lombok.NonNull;

/**
 * Provides an implementation of a GeoGig object database that utilizes the heap for the storage of
 * objects.
 * 
 * @see ForwardingObjectStore
 */
public class HeapObjectDatabase extends ForwardingObjectStore implements ObjectDatabase {

    private final HeapBlobStore blobs;

    private final HeapGraphDatabase graph;

    public HeapObjectDatabase() {
        this(null);
    }

    public HeapObjectDatabase(Hints hints) {
        super(new HeapObjectStore(Hints.isRepoReadOnly(hints)));
        blobs = new HeapBlobStore();
        graph = new HeapGraphDatabase(isReadOnly());
    }

    /**
     * Closes the database.
     * 
     * @see org.locationtech.geogig.storage.ObjectDatabase#close()
     */
    public @Override void close() {
        if (isOpen()) {
            super.close();
            graph.close();
        }
    }

    /**
     * Opens the database for use by GeoGig.
     */
    public @Override void open() {
        if (isOpen()) {
            return;
        }
        super.open();
        graph.open();
    }

    public @Override BlobStore getBlobStore() {
        return blobs;
    }

    public @Override GraphDatabase getGraphDatabase() {
        return graph;
    }

    public @Override boolean put(@NonNull RevObject object) {
        final boolean added = super.put(object);
        if (added && TYPE.COMMIT.equals(object.getType())) {
            try {
                RevCommit c = (RevCommit) object;
                graph.put(c.getId(), c.getParentIds());
            } catch (RuntimeException e) {
                super.delete(object.getId());
                throw e;
            }
        }
        return added;
    }

    public @Override void putAll(@NonNull Stream<? extends RevObject> objects,
            @NonNull BulkOpListener listener) {
        checkState(isOpen(), "db is closed");
        checkWritable();

        objects.forEach((o) -> {
            if (put(o)) {
                listener.inserted(o.getId(), null);
            } else {
                listener.found(o.getId(), null);
            }
        });
    }

    public @Override String toString() {
        return getClass().getSimpleName();
    }
}
