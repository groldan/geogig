/* Copyright (c) 2014 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Justin Deoliveira (Boundless) - initial implementation
 */
package org.locationtech.geogig.storage.sqlite;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.api.ObjectId;
import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.api.RevCommit;
import org.locationtech.geogig.api.RevFeature;
import org.locationtech.geogig.api.RevFeatureType;
import org.locationtech.geogig.api.RevObject;
import org.locationtech.geogig.api.RevTag;
import org.locationtech.geogig.api.RevTree;
import org.locationtech.geogig.di.VersionedFormat;
import org.locationtech.geogig.repository.Hints;
import org.locationtech.geogig.repository.RepositoryConnectionException;
import org.locationtech.geogig.storage.BulkOpListener;
import org.locationtech.geogig.storage.ConfigDatabase;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.locationtech.geogig.storage.ObjectInserter;
import org.locationtech.geogig.storage.ObjectSerializingFactory;
import org.locationtech.geogig.storage.datastream.DataStreamSerializationFactoryV1;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

/**
 * Base class for SQLite based object database.
 * 
 * @author Justin Deoliveira, Boundless
 * 
 * @param <C> Connection type.
 */
public abstract class SQLiteObjectDatabase<C> implements ObjectDatabase {

    final Platform platform;

    final ConfigDatabase configdb;

    private final VersionedFormat version;

    final ObjectSerializingFactory serializer = DataStreamSerializationFactoryV1.INSTANCE;

    protected C cx;

    private boolean readOnly;

    public SQLiteObjectDatabase(final ConfigDatabase configdb, final Platform platform,
            final @Nullable Hints hints, final VersionedFormat version) {
        this.configdb = configdb;
        this.platform = platform;
        this.version = version;
        this.readOnly = readOnly(hints);
    }

    private static boolean readOnly(Hints hints) {
        return hints == null ? false : hints.getBoolean(Hints.OBJECTS_READ_ONLY);
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public void open() {
        if (cx == null) {
            cx = connect(SQLiteStorage.geogigDir(platform));
            init(cx);
        }
    }

    @Override
    public void configure() throws RepositoryConnectionException {
        RepositoryConnectionException.StorageType.OBJECT.configure(configdb, version.getFormat(),
                version.getVersion());
    }

    @Override
    public void checkConfig() throws RepositoryConnectionException {
        RepositoryConnectionException.StorageType.OBJECT.verify(configdb, version.getFormat(),
                version.getVersion());
    }

    @Override
    public boolean isOpen() {
        return cx != null;
    }

    @Override
    public void close() {
        if (cx != null) {
            close(cx);
            cx = null;
        }
    }

    @Override
    public boolean exists(ObjectId id) {
        Preconditions.checkNotNull(id, "id is null");
        checkOpen();
        return has(id, cx);
    }

    @Override
    public List<ObjectId> lookUp(String partialId) {
        checkNotNull(partialId, "argument partialId is null");
        checkArgument(partialId.length() > 7, "partial id must be at least 8 characters long: ",
                partialId);
        checkOpen();
        return Lists.newArrayList(transform(search(partialId, cx), StringToObjectId.INSTANCE));
    }

    @Override
    public RevObject get(ObjectId id) throws IllegalArgumentException {
        Preconditions.checkNotNull(id, "id is null");
        checkOpen();
        RevObject obj = getIfPresent(id);
        if (obj == null) {
            throw new IllegalArgumentException("Object " + id + " does not exist");
        }

        return obj;
    }

    @Override
    public <T extends RevObject> T get(ObjectId id, Class<T> type) throws IllegalArgumentException {
        Preconditions.checkNotNull(id, "id is null");
        Preconditions.checkNotNull(type, "type is null");
        checkOpen();
        T obj = getIfPresent(id, type);
        if (obj == null) {
            throw new IllegalArgumentException("Object " + id + " does not exist");
        }
        return obj;
    }

    @Override
    public RevObject getIfPresent(ObjectId id) {
        Preconditions.checkNotNull(id, "id is null");
        checkOpen();
        InputStream bytes = get(id, cx);
        try {
            return readObject(bytes, id);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public <T extends RevObject> T getIfPresent(ObjectId id, Class<T> type)
            throws IllegalArgumentException {
        Preconditions.checkNotNull(id, "id is null");
        Preconditions.checkNotNull(type, "type is null");
        checkOpen();
        RevObject obj = getIfPresent(id);

        if (!type.isInstance(obj)) {
            return null;
        }

        return obj != null ? type.cast(obj) : null;
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
    public Iterator<RevObject> getAll(Iterable<ObjectId> ids) {
        return getAll(ids, BulkOpListener.NOOP_LISTENER);
    }

    @Override
    public Iterator<RevObject> getAll(Iterable<ObjectId> ids, final BulkOpListener listener) {
        Preconditions.checkNotNull(ids, "ids is null");
        Preconditions.checkNotNull(listener, "listener is null");
        checkOpen();
        return filter(transform(ids, new Function<ObjectId, RevObject>() {
            @Override
            public RevObject apply(ObjectId id) {
                RevObject obj = getIfPresent(id);
                if (obj == null) {
                    listener.notFound(id);
                } else {
                    listener.found(id, null);
                }
                return obj;
            }
        }), Predicates.notNull()).iterator();
    }

    @Override
    public boolean put(RevObject object) {
        checkNotNull(object, "argument object is null");
        checkArgument(!object.getId().isNull(), "ObjectId is NULL %s", object);
        checkWritable();
        ObjectId id = object.getId();
        try {
            return put(id, writeObject(object), cx);
        } catch (IOException e) {
            throw new RuntimeException("Unable to serialize object: " + object);
        }
    }

    @Override
    public void putAll(Iterator<? extends RevObject> objects) {
        putAll(objects, BulkOpListener.NOOP_LISTENER);
    }

    @Override
    public void putAll(Iterator<? extends RevObject> objects, BulkOpListener listener) {
        checkNotNull(objects, "objects is null");
        checkNotNull(listener, "listener is null");
        checkWritable();
        while (objects.hasNext()) {
            RevObject obj = objects.next();
            if (put(obj)) {
                listener.inserted(obj.getId(), null);
            }
        }
    }

    @Override
    public boolean delete(ObjectId objectId) {
        Preconditions.checkNotNull(objectId, "argument id is null");
        checkWritable();
        return delete(objectId, cx);
    }

    @Override
    public long deleteAll(Iterator<ObjectId> ids) {
        return deleteAll(ids, BulkOpListener.NOOP_LISTENER);
    }

    @Override
    public long deleteAll(Iterator<ObjectId> ids, BulkOpListener listener) {
        Preconditions.checkNotNull(ids, "argument ids is null");
        Preconditions.checkNotNull(listener, "argument listener is null");
        checkWritable();
        long count = 0;
        while (ids.hasNext()) {
            ObjectId id = ids.next();
            if (delete(id)) {
                count++;
                listener.deleted(id);
            }
        }
        return count;
    }

    @Override
    public ObjectInserter newObjectInserter() {
        return new ObjectInserter(this);
    }

    /**
     * Reads object from its binary representation as stored in the database.
     * 
     * @throws IOException
     */
    protected RevObject readObject(InputStream bytes, ObjectId id) throws IOException {
        if (bytes == null) {
            return null;
        }

        return serializer.read(id, bytes);
    }

    /**
     * Writes object to its binary representation as stored in the database.
     */
    protected InputStream writeObject(RevObject object) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();

        serializer.write(object, bout);
        return new ByteArrayInputStream(bout.toByteArray());
    }

    protected byte[] writeObject2(RevObject object) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();

        serializer.write(object, bout);

        return bout.toByteArray();
    }

    protected void checkWritable() {
        checkOpen();
        if (readOnly) {
            throw new IllegalStateException("database is read only.");
        }
    }

    protected void checkOpen() {
        Preconditions.checkState(isOpen(), "Database is closed");
    }

    /**
     * Opens a database connection, returning the object representing connection state.
     */
    protected abstract C connect(File geogigDir);

    /**
     * Closes a database connection.
     * 
     * @param cx The connection object.
     */
    protected abstract void close(C cx);

    /**
     * Creates the object table with the following schema:
     * 
     * <pre>
     * objects(id:varchar PRIMARY KEY, object:blob)
     * </pre>
     * 
     * Implementations of this method should be prepared to be called multiple times, so must check
     * if the table already exists.
     * 
     * @param cx The connection object.
     */
    protected abstract void init(C cx);

    /**
     * Determines if the object with the specified id exists.
     */
    protected abstract boolean has(ObjectId id, C cx);

    /**
     * Searches for objects with ids that match the speciifed partial string.
     * 
     * @param partialId The partial id.
     * 
     * @return Iterable of matches.
     */
    protected abstract Iterable<String> search(String partialId, C cx);

    /**
     * Retrieves the object with the specified id.
     * <p>
     * Must return <code>null</code> if no such object exists.
     * </p>
     */
    protected abstract InputStream get(ObjectId id, C cx);

    /**
     * Inserts or updates the object with the specified id.
     */
    protected abstract boolean put(ObjectId id, InputStream obj, C cx);

    /**
     * Deletes the object with the specified id.
     * 
     * @return Flag indicating if object was actually removed.
     */
    protected abstract boolean delete(ObjectId id, C cx);
}
