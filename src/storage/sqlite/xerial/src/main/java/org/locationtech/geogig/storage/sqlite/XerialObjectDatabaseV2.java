/* Copyright (c) 2014 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.storage.sqlite;

import static java.lang.String.format;
import static org.locationtech.geogig.storage.sqlite.Xerial.log;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.api.ObjectId;
import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.api.RevObject;
import org.locationtech.geogig.repository.Hints;
import org.locationtech.geogig.storage.BlobStore;
import org.locationtech.geogig.storage.BulkOpListener;
import org.locationtech.geogig.storage.ConfigDatabase;
import org.locationtech.geogig.storage.fs.FileBlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteDataSource;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * Object database based on Xerial SQLite jdbc driver.
 */
public class XerialObjectDatabaseV2 extends SQLiteObjectDatabase<DataSource> {

    static Logger LOG = LoggerFactory.getLogger(XerialObjectDatabaseV2.class);

    static final String OBJECTS = "objects";

    final int partitionSize = 50; // TODO make configurable

    final String dbName;

    private XerialConflictsDatabaseV2 conflicts;

    private FileBlobStore blobStore;

    private SQLiteTransactionHandler txHandler;

    @Inject
    public XerialObjectDatabaseV2(final ConfigDatabase configdb, final Platform platform,
            final @Nullable Hints hints) {
        super(configdb, platform, hints, XerialStorageProviderV2.OBJECT);
        this.dbName = "objects";
    }

    // StackTraceElement[] openingCallers;

    @Override
    protected DataSource connect(File geogigDir) {
        // openingCallers = Thread.currentThread().getStackTrace();
        SQLiteDataSource dataSource = Xerial.newDataSource(new File(geogigDir, dbName + ".db"));

        HikariConfig poolConfig = new HikariConfig();
        poolConfig.setMaximumPoolSize(20);
        poolConfig.setDataSource(dataSource);
        poolConfig.setMinimumIdle(0);
        poolConfig.setIdleTimeout(TimeUnit.SECONDS.toMillis(10));

        HikariDataSource connPool = new HikariDataSource(poolConfig);
        this.txHandler = new SQLiteTransactionHandler(connPool);

        return connPool;
    }

    @Override
    protected void close(DataSource ds) {
        if (this.txHandler != null) {
            this.txHandler.close();
            this.txHandler = null;
        }
        ((HikariDataSource) ds).close();
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            if (this.txHandler != null) {
                System.err.println("----------------------------------------------");
                System.err.println("---------------- WARNING ---------------------");
                System.err.printf("---- DATABASE '%s/%s' has not been closed ------\n",
                        super.platform.pwd(), dbName + ".db");
                // for (StackTraceElement e : openingCallers) {
                // System.err.println(e.toString());
                // }
                System.err.println("----------------------------------------------");
                close();
            }
        } finally {
            super.finalize();
        }
    }

    @Override
    public void init(DataSource ds) {

        try (Connection c = ds.getConnection()) {
            try (Statement stmt = c.createStatement()) {

                // stmt.execute("PRAGMA page_size = 8096");
                stmt.execute("PRAGMA journal_mode=WAL");
                // stmt.execute("PRAGMA wal_autocheckpoint=1000");
                // stmt.execute("wal_checkpoint(TRUNCATE)");
                int cacheSizeKB = 1024 * 512;// 512MB
                // the minus sign in "cache_size = -<N>" indicates the cache size is expressed in KB
                // instead of number of pages
                stmt.execute("PRAGMA cache_size = -" + cacheSizeKB);
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        runTx(new SQLiteTransactionHandler.WriteOp<Void>() {

            @Override
            protected Void doRun(Connection cx) throws SQLException {
                // REMEMBER: if setting 'PRAGMA journal_mode=WAL', can't create a table with WITHOUT
                // ROWID, cause the b-tree stores data in both leaf and intermediate nodes,
                // __exploding__ the size of the WAL file. Badly.
                String createTable = format(
                        "CREATE TABLE IF NOT EXISTS %s (inthash int NOT NULL, id blob NOT NULL, object blob NOT NULL)",
                        OBJECTS);
                String createIndex = format(
                        "CREATE INDEX IF NOT EXISTS %s_inthash_idx ON %s(inthash) ", OBJECTS,
                        OBJECTS);

                try (Statement stmt = cx.createStatement()) {
                    stmt.execute(log(createTable, LOG));
                    stmt.execute(log(createIndex, LOG));
                }
                return null;
            }
        });

        conflicts = new XerialConflictsDatabaseV2(ds, txHandler);
        conflicts.open();
        blobStore = new FileBlobStore(platform);
        blobStore.open();
    }

    @Override
    public XerialConflictsDatabaseV2 getConflictsDatabase() {
        return conflicts;
    }

    @Override
    public BlobStore getBlobStore() {
        return blobStore;
    }

    @Override
    public boolean has(final ObjectId id, DataSource ds) {

        return new DbOp<Boolean>() {
            @Override
            protected Boolean doRun(Connection cx) throws SQLException {
                String sql = format("SELECT id FROM %s WHERE inthash = ?", OBJECTS);

                int intHash = intHash(id);
                try (PreparedStatement ps = cx.prepareStatement(log(sql, LOG, intHash, id))) {
                    ps.setInt(1, intHash);

                    try (ResultSet rs = ps.executeQuery()) {
                        boolean found = seek(rs, id, 1);
                        return found;
                    }
                }
            }
        }.run(ds);
    }

    private boolean seek(ResultSet rs, ObjectId id, int idFieldIndex) throws SQLException {
        final byte[] rawId = id.getRawValue();
        while (rs.next()) {
            byte[] dbId = rs.getBytes(idFieldIndex);
            if (Arrays.equals(rawId, dbId)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Iterable<String> search(final String partialId, DataSource ds) {

        final byte[] raw = ObjectId.toRaw(partialId);
        Preconditions.checkArgument(raw.length >= 4,
                "Partial id must be at least 8 characters long: %s", partialId);

        final int inthash = intHash(raw);

        Iterable<String> matches = new DbOp<Iterable<String>>() {
            @Override
            protected Iterable<String> doRun(Connection cx) throws IOException, SQLException {
                String sql = format("SELECT id FROM %s WHERE inthash = ? AND id LIKE ? LIMIT 100",
                        OBJECTS, partialId);
                final String searchKey = partialId + "%";
                try (PreparedStatement ps = cx.prepareStatement(log(sql, LOG, inthash, searchKey))) {
                    ps.setInt(1, inthash);
                    ps.setString(2, searchKey);

                    List<String> matchList = new ArrayList<>();
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            matchList.add(rs.getString(1));
                        }
                    }
                    return matchList;
                }
            }
        }.run(ds);
        return matches;
    }

    @Override
    public InputStream get(final ObjectId id, DataSource ds) {
        return new DbOp<InputStream>() {
            @Override
            protected InputStream doRun(Connection cx) throws SQLException {
                String sql = format("SELECT id, object FROM %s WHERE inthash = ?", OBJECTS);

                int intHash = intHash(id);

                try (PreparedStatement ps = cx.prepareStatement(log(sql, LOG, intHash, id))) {
                    ps.setInt(1, intHash);

                    try (ResultSet rs = ps.executeQuery()) {
                        boolean found = seek(rs, id, 1);
                        if (found) {
                            byte[] bytes = rs.getBytes(2);
                            return new ByteArrayInputStream(bytes);
                        }
                        return null;
                    }
                }
            }
        }.run(ds);
    }

    @Override
    public void put(final ObjectId id, final InputStream obj, DataSource ds) {

        runTx(new SQLiteTransactionHandler.WriteOp<Void>() {
            @Override
            protected Void doRun(Connection cx) throws IOException, SQLException {
                String sql = format("INSERT OR IGNORE INTO %s (inthash,id,object) VALUES (?,?,?)",
                        OBJECTS);

                int intHash = intHash(id);

                try (PreparedStatement ps = cx.prepareStatement(log(sql, LOG, intHash, id, obj))) {
                    ps.setInt(1, intHash);
                    ps.setBytes(2, id.getRawValue());
                    ps.setBytes(3, ByteStreams.toByteArray(obj));
                    ps.executeUpdate();
                    return null;
                }
            }
        });
    }

    @Override
    public boolean delete(final ObjectId id, DataSource ds) {
        return runTx(new SQLiteTransactionHandler.WriteOp<Boolean>() {
            @Override
            protected Boolean doRun(Connection cx) throws SQLException {
                String sql = format("DELETE FROM %s WHERE inthash = ? AND id = ?", OBJECTS);

                int intHash = intHash(id);

                try (PreparedStatement ps = cx.prepareStatement(log(sql, LOG, intHash, id))) {
                    ps.setInt(1, intHash);
                    ps.setString(2, id.toString());
                    return ps.executeUpdate() > 0;
                }
            }
        });
    }

    private <T> T runTx(final SQLiteTransactionHandler.WriteOp<T> dbop) {
        return txHandler.runTx(dbop);
    }

    /**
     * Override to optimize batch insert.
     */
    @Override
    public void putAll(Iterator<? extends RevObject> objects, final BulkOpListener listener) {
        Preconditions.checkState(isOpen(), "No open database connection");

        // System.err.println("put allllllll");
        txHandler.startTransaction();
        while (objects.hasNext()) {

            final Iterator<? extends RevObject> objs = Iterators.limit(objects, partitionSize);

            runTx(new SQLiteTransactionHandler.WriteOp<Void>() {

                @Override
                protected Void doRun(Connection cx) throws IOException, SQLException {
                    // use INSERT OR IGNORE to deal with duplicates cleanly
                    String sql = format(
                            "INSERT OR IGNORE INTO %s (inthash, id, object) VALUES (?,?,?)",
                            OBJECTS);

                    // Stopwatch sw = Stopwatch.createStarted();
                    List<ObjectId> ids = new LinkedList<>();
                    try (PreparedStatement stmt = cx.prepareStatement(log(sql, LOG))) {
                        while (objs.hasNext()) {
                            RevObject obj = objs.next();
                            ObjectId id = obj.getId();
                            ids.add(id);
                            stmt.setInt(1, intHash(id));
                            stmt.setBytes(2, id.getRawValue());
                            stmt.setBytes(3, ByteStreams.toByteArray(writeObject(obj)));
                            stmt.addBatch();
                        }

                        int[] batchResults = stmt.executeBatch();
                        notifyInserted(batchResults, ids, listener);
                        stmt.clearParameters();
                    }
                    // System.err.printf("wrote %,d objects in %s on thread %s\n", ids.size(),
                    // sw.stop(), Thread.currentThread().getName());
                    return null;
                }
            });
        }
        txHandler.endTransaction();
    }

    void notifyInserted(int[] inserted, List<ObjectId> objects, BulkOpListener listener) {
        for (int i = 0; i < inserted.length; i++) {
            if (inserted[i] > 0) {
                listener.inserted(objects.get(i), null);
            }
        }
    }

    /**
     * Override to optimize batch delete.
     */
    @Override
    public long deleteAll(Iterator<ObjectId> ids, final BulkOpListener listener) {
        Preconditions.checkState(isOpen(), "No open database connection");

        txHandler.startTransaction();
        long totalCount = 0;
        while (ids.hasNext()) {

            final Iterator<ObjectId> deleteIds = Iterators.limit(ids, partitionSize);

            totalCount += runTx(new SQLiteTransactionHandler.WriteOp<Long>() {

                @Override
                protected Long doRun(Connection cx) throws IOException, SQLException {
                    String sql = format("DELETE FROM %s WHERE inthash = ? AND id = ?", OBJECTS);

                    long count = 0;
                    try (PreparedStatement stmt = cx.prepareStatement(log(sql, LOG))) {

                        // partition the objects into chunks for batch processing
                        List<ObjectId> ids = Lists.newArrayList(deleteIds);

                        for (ObjectId id : ids) {
                            stmt.setInt(1, intHash(id));
                            stmt.setString(2, id.toString());
                            stmt.addBatch();
                        }
                        count += notifyDeleted(stmt.executeBatch(), ids, listener);
                        stmt.clearParameters();
                    }
                    return count;
                }
            }).longValue();

        }
        txHandler.endTransaction();

        return totalCount;
    }

    long notifyDeleted(int[] deleted, List<ObjectId> ids, BulkOpListener listener) {
        long count = 0;
        for (int i = 0; i < deleted.length; i++) {
            if (deleted[i] > 0) {
                count++;
                listener.deleted(ids.get(i));
            }
        }
        return count;
    }

    public static int intHash(ObjectId id) {
        int hash1 = (id.byteN(0) << 24) //
                + (id.byteN(1) << 16) //
                + (id.byteN(2) << 8) //
                + (id.byteN(3) << 0);
        return hash1;
    }

    public static int intHash(byte[] id) {
        int hash1 = (id[0] << 24) //
                + (id[1] << 16) //
                + (id[2] << 8) //
                + (id[3] << 0);
        return hash1;
    }
}
