/* Copyright (c) 2015 Boundless and others.
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
import com.google.common.io.ByteArrayDataOutput;
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

    static final String INSERT_SQL = format("INSERT INTO %s (inthash,h2,h3,object) SELECT ?,?,?,?" + //
            " WHERE NOT EXISTS (SELECT 1 FROM %s WHERE inthash=? AND h2=? AND h3=?)", OBJECTS,
            OBJECTS);

    static final String DELETE_SQL = format(
            "DELETE FROM %s WHERE inthash = ? AND h2 = ? AND h3 = ?", OBJECTS);

    static final String SELECT_SQL = format(
            "SELECT object FROM %s WHERE inthash = ? AND h2=? AND h3=?", OBJECTS);

    static final String LOOKUP_QUERY = format("SELECT inthash, h2, h3 FROM %s WHERE inthash = ?",
            OBJECTS);

    static final String EXISTS_QUERY = format(
            "SELECT 1 FROM %s WHERE inthash = ? AND h2 = ? AND h3 = ?", OBJECTS);

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
                        "CREATE TABLE IF NOT EXISTS %s "
                                + "(inthash INTEGER NOT NULL, h2 INTEGER NOT NULL, h3 INTEGER NOT NULL, object blob NOT NULL)",
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
                // "SELECT 1 FROM %s WHERE inthash = ? AND h2 = ? AND h3 = ?",OBJECTS);

                final ID dbId = ID.valueOf(id);

                try (PreparedStatement ps = cx.prepareStatement(EXISTS_QUERY)) {
                    ps.setInt(1, dbId.hash1());
                    ps.setLong(2, dbId.hash2());
                    ps.setLong(3, dbId.hash3());
                    try (ResultSet rs = ps.executeQuery()) {
                        boolean found = rs.next();
                        return found;
                    }
                }
            }
        }.run(ds);
    }

    @Override
    public Iterable<String> search(final String partialId, DataSource ds) {

        final byte[] raw = ObjectId.toRaw(partialId);
        Preconditions.checkArgument(raw.length >= 4,
                "Partial id must be at least 8 characters long: %s", partialId);

        final int inthash = ID.intHash(raw);

        Iterable<String> matches = new DbOp<Iterable<String>>() {
            @Override
            protected Iterable<String> doRun(Connection cx) throws IOException, SQLException {
                // "SELECT inthash, h2, h3, object FROM %s WHERE inthash = ?", OBJECTS);

                final String sql = LOOKUP_QUERY;
                try (PreparedStatement ps = cx.prepareStatement(sql)) {
                    ps.setInt(1, inthash);

                    List<String> matchList = new ArrayList<>();
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            ObjectId id = ID.valueOf(rs.getInt(1), rs.getLong(2), rs.getLong(3))
                                    .toObjectId();
                            String stringId = id.toString();
                            if (stringId.startsWith(partialId)) {
                                matchList.add(stringId);
                            }
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
                // "SELECT object FROM %s WHERE inthash = ? AND h2=? AND h3=?",
                // OBJECTS);

                final ID dbId = ID.valueOf(id);

                try (PreparedStatement ps = cx.prepareStatement(SELECT_SQL)) {
                    ps.setInt(1, dbId.hash1());
                    ps.setLong(2, dbId.hash2());
                    ps.setLong(3, dbId.hash3());

                    try (ResultSet rs = ps.executeQuery()) {
                        final boolean found = rs.next();
                        if (found) {
                            byte[] bytes = rs.getBytes(1);
                            return new ByteArrayInputStream(bytes);
                        }
                        return null;
                    }
                }
            }
        }.run(ds);
    }

    @Override
    public boolean put(final ObjectId id, final InputStream obj, DataSource ds) {

        return runTx(new SQLiteTransactionHandler.WriteOp<Boolean>() {
            @Override
            protected Boolean doRun(Connection cx) throws IOException, SQLException {
                // String sql =
                // format("INSERT OR IGNORE INTO %s (inthash,id,object) VALUES (?,?,?)",
                // OBJECTS);

                // String sql = format("INSERT INTO %s (inthash,h2,h3,object) SELECT ?,?,?,?" + //
                // " WHERE NOT EXISTS (SELECT 1 FROM %s WHERE inthash=? AND h2=? AND h3=?)",
                // OBJECTS, OBJECTS);

                final ID dbId = ID.valueOf(id);

                try (PreparedStatement ps = cx.prepareStatement(INSERT_SQL)) {
                    ps.setInt(1, dbId.hash1());
                    ps.setLong(2, dbId.hash2());
                    ps.setLong(3, dbId.hash3());
                    ps.setBytes(4, ByteStreams.toByteArray(obj));
                    ps.setInt(5, dbId.hash1());
                    ps.setLong(6, dbId.hash2());
                    ps.setLong(7, dbId.hash3());

                    int updateCount = ps.executeUpdate();
                    return Boolean.valueOf(updateCount > 0);
                }
            }
        });
    }

    @Override
    public boolean delete(final ObjectId id, DataSource ds) {
        return runTx(new SQLiteTransactionHandler.WriteOp<Boolean>() {
            @Override
            protected Boolean doRun(Connection cx) throws SQLException {
                // "DELETE FROM %s WHERE inthash = ? AND h2 = ? AND h3 = ?", OBJECTS);

                final ID dbId = ID.valueOf(id);

                try (PreparedStatement ps = cx.prepareStatement(DELETE_SQL)) {
                    ps.setInt(1, dbId.hash1());
                    ps.setLong(2, dbId.hash2());
                    ps.setLong(3, dbId.hash3());
                    final int updateCount = ps.executeUpdate();
                    return updateCount > 0;
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
        Preconditions.checkNotNull(objects, "argument objects is null");
        Preconditions.checkNotNull(listener, "argument listener is null");
        checkWritable();

        // System.err.println("put allllllll");
        txHandler.startTransaction();
        while (objects.hasNext()) {

            final Iterator<? extends RevObject> objs = Iterators.limit(objects, partitionSize);

            runTx(new SQLiteTransactionHandler.WriteOp<Void>() {

                @Override
                protected Void doRun(Connection cx) throws IOException, SQLException {
                    // String sql = format(
                    // "INSERT OR IGNORE INTO %s (inthash, id, object) VALUES (?,?,?)",
                    // OBJECTS);

                    // final String sql = format(
                    // "INSERT INTO %s (inthash,h2,h3,object) SELECT ?,?,?,?" + //
                    // " WHERE NOT EXISTS (SELECT 1 FROM %s WHERE inthash=? AND h2=? AND h3=?)",
                    // OBJECTS, OBJECTS);

                    // Stopwatch sw = Stopwatch.createStarted();

                    final String sql = INSERT_SQL;
                    List<ObjectId> ids = new LinkedList<>();
                    try (PreparedStatement ps = cx.prepareStatement(sql)) {
                        while (objs.hasNext()) {
                            RevObject obj = objs.next();
                            ObjectId id = obj.getId();
                            ids.add(id);

                            final ID dbId = ID.valueOf(id);

                            ps.setInt(1, dbId.hash1());
                            ps.setLong(2, dbId.hash2());
                            ps.setLong(3, dbId.hash3());
                            ps.setBytes(4, writeObject2(obj));
                            ps.setInt(5, dbId.hash1());
                            ps.setLong(6, dbId.hash2());
                            ps.setLong(7, dbId.hash3());

                            ps.addBatch();
                        }

                        int[] batchResults = ps.executeBatch();
                        notifyInserted(batchResults, ids, listener);
                        ps.clearParameters();
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
            } else {
                listener.found(objects.get(i), null);
            }
        }
    }

    /**
     * Override to optimize batch delete.
     */
    @Override
    public long deleteAll(Iterator<ObjectId> ids, final BulkOpListener listener) {
        Preconditions.checkNotNull(ids, "argument ids is null");
        Preconditions.checkNotNull(listener, "argument listener is null");
        checkWritable();

        txHandler.startTransaction();
        long totalCount = 0;
        while (ids.hasNext()) {

            final Iterator<ObjectId> deleteIds = Iterators.limit(ids, partitionSize);

            totalCount += runTx(new SQLiteTransactionHandler.WriteOp<Long>() {

                @Override
                protected Long doRun(Connection cx) throws IOException, SQLException {

                    long count = 0;
                    final String sql = DELETE_SQL;
                    try (PreparedStatement stmt = cx.prepareStatement(sql)) {

                        // partition the objects into chunks for batch processing
                        List<ObjectId> ids = Lists.newArrayList(deleteIds);

                        for (ObjectId id : ids) {
                            final ID dbId = ID.valueOf(id);
                            stmt.setInt(1, dbId.hash1());
                            stmt.setLong(2, dbId.hash2());
                            stmt.setLong(3, dbId.hash3());
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
            } else {
                listener.notFound(ids.get(i));
            }
        }
        return count;
    }

    static final class ID {

        private final byte[] id;

        public ID(byte[] oid) {
            this.id = oid;
        }

        public static int intHash(ObjectId id) {
            final int hash1 = ((id.byteN(0) << 24) //
                    | (id.byteN(1) << 16) //
                    | (id.byteN(2) << 8) //
            | (id.byteN(3)));
            return hash1;
        }

        public static int intHash(byte[] id) {
            final int hash1 = ((((int) id[0]) << 24) //
                    | (((int) id[1] & 0xFF) << 16) //
                    | (((int) id[2] & 0xFF) << 8) //
            | (((int) id[3] & 0xFF)));
            return hash1;
        }

        public int hash1() {
            return ID.intHash(this.id);
        }

        public long hash2() {
            final long hash2 = ((((long) id[4]) << 56) //
                    | (((long) id[5] & 0xFF) << 48)//
                    | (((long) id[6] & 0xFF) << 40) //
                    | (((long) id[7] & 0xFF) << 32) //
                    | (((long) id[8] & 0xFF) << 24) //
                    | (((long) id[9] & 0xFF) << 16) //
                    | (((long) id[10] & 0xFF) << 8)//
            | (((long) id[11] & 0xFF)));
            return hash2;
        }

        public long hash3() {
            final long hash3 = ((((long) id[12]) << 56) //
                    | (((long) id[13] & 0xFF) << 48)//
                    | (((long) id[14] & 0xFF) << 40) //
                    | (((long) id[15] & 0xFF) << 32) //
                    | (((long) id[16] & 0xFF) << 24) //
                    | (((long) id[17] & 0xFF) << 16) //
                    | (((long) id[18] & 0xFF) << 8)//
            | (((long) id[19] & 0xFF)));
            return hash3;
        }

        public ObjectId toObjectId() {
            return ObjectId.createNoClone(id);
        }

        public static ID valueOf(ObjectId oid) {
            return valueOf(oid.getRawValue());
        }

        public static ID valueOf(byte[] oid) {
            return new ID(oid);
        }

        public static ID valueOf(final int h1, final long h2, final long h3) {
            ByteArrayDataOutput out = ByteStreams.newDataOutput();
            out.writeInt(h1);
            out.writeLong(h2);
            out.writeLong(h3);
            byte[] raw = out.toByteArray();
            return new ID(raw);
        }

        @Override
        public String toString() {
            return String.format("ID[%d, %d, %d]", hash1(), hash2(), hash3());
        }
    }
}
