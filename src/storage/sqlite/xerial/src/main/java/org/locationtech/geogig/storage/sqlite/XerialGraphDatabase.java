/* Copyright (c) 2014-2016 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Justin Deoliveira (Boundless) - initial implementation
 */
package org.locationtech.geogig.storage.sqlite;

import static org.locationtech.geogig.storage.sqlite.Xerial.log;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.di.VersionedFormat;
import org.locationtech.geogig.storage.ConfigDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteDataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import static java.lang.String.format;

/**
 * Graph database based on xerial SQLite jdbc driver.
 * 
 * @author Justin Deoliveira, Boundless
 */
abstract class XerialGraphDatabase extends SQLiteGraphDatabase<DataSource> {

    static Logger LOG = LoggerFactory.getLogger(XerialGraphDatabase.class);

    static final String NODES = "nodes";

    static final String EDGES = "edges";

    static final String PROPS = "props";

    static final String MAPPINGS = "mappings";

    static final String DBNAME = "graph.db";

    private SQLiteTransactionHandler txHandler;

    public XerialGraphDatabase(ConfigDatabase configdb, Platform platform, VersionedFormat version) {
        super(configdb, platform, version);
    }

    @Override
    protected DataSource connect(File geogigDir) {
        SQLiteDataSource dataSource = Xerial.newDataSource(new File(geogigDir, DBNAME));

        HikariConfig poolConfig = new HikariConfig();
        poolConfig.setMaximumPoolSize(10);
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
                        super.platform.pwd(), DBNAME);
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
        runTx(new SQLiteTransactionHandler.WriteOp<Void>() {
            @Override
            protected Void doRun(Connection cx) throws IOException, SQLException {
                try (Statement st = cx.createStatement()) {

                    String sql = format("CREATE TABLE IF NOT EXISTS %s (id VARCHAR PRIMARY KEY)",
                            NODES);
                    st.execute(log(sql, LOG));

                    sql = format("CREATE TABLE IF NOT EXISTS %s (src VARCHAR, dst VARCHAR, "
                            + "PRIMARY KEY (src,dst))", EDGES);
                    st.execute(log(sql, LOG));

                    sql = format("CREATE INDEX IF NOT EXISTS %s_src_index ON %s(src)", EDGES, EDGES);
                    st.execute(log(sql, LOG));

                    sql = format("CREATE INDEX IF NOT EXISTS %s_dst_index ON %s(dst)", EDGES, EDGES);
                    st.execute(log(sql, LOG));

                    sql = format(
                            "CREATE TABLE IF NOT EXISTS %s (nid VARCHAR, key VARCHAR, val VARCHAR,"
                                    + " PRIMARY KEY(nid,key))", PROPS);
                    st.execute(log(sql, LOG));

                    sql = format(
                            "CREATE TABLE IF NOT EXISTS %s (alias VARCHAR PRIMARY KEY, nid VARCHAR)",
                            MAPPINGS);
                    st.execute(log(sql, LOG));

                    sql = format("CREATE INDEX IF NOT EXISTS %s_nid_index ON %s(nid)", MAPPINGS,
                            MAPPINGS);
                    st.execute(log(sql, LOG));
                }
                return null;
            }
        });

    }

    private <T> T runTx(final SQLiteTransactionHandler.WriteOp<T> dbop) {
        return txHandler.runTx(dbop);
    }

    @Override
    public boolean put(final String node, DataSource ds) {

        return runTx(new SQLiteTransactionHandler.WriteOp<Boolean>() {
            @Override
            protected Boolean doRun(Connection cx) throws IOException, SQLException {
                String sql = format("INSERT OR IGNORE INTO %s (id) VALUES (?)", NODES);

                //cx.setAutoCommit(false);
                try (PreparedStatement ps = cx.prepareStatement(log(sql, LOG, node))) {
                    ps.setString(1, node);
                    int updateCount = ps.executeUpdate();
                    //cx.commit();
                    return Boolean.valueOf(updateCount > 0);
                }
            }
        });
    }

    @Override
    public boolean has(final String node, DataSource ds) {
        return new DbOp<Boolean>() {
            @Override
            protected Boolean doRun(Connection cx) throws IOException, SQLException {
                String sql = format("SELECT count(*) FROM %s WHERE id = ?", NODES);

                try (PreparedStatement ps = cx.prepareStatement(log(sql, LOG, node))) {
                    ps.setString(1, node);

                    try (ResultSet rs = ps.executeQuery()) {
                        rs.next();
                        int count = rs.getInt(1);
                        return count > 0;
                    }
                }
            }
        }.run(ds);
    }

    @Override
    public void relate(final String src, final String dst, DataSource ds) {
        runTx(new SQLiteTransactionHandler.WriteOp<Void>() {
            @Override
            protected Void doRun(Connection cx) throws IOException, SQLException {
                String sql = format("INSERT or IGNORE INTO %s (src, dst) VALUES (?, ?)", EDGES);

                try (PreparedStatement ps = cx.prepareStatement(log(sql, LOG, src, dst))) {
                    ps.setString(1, src);
                    ps.setString(2, dst);

                    ps.executeUpdate();
                }
                return null;
            }
        });
    }

    @Override
    public void map(final String from, final String to, DataSource ds) {
        runTx(new SQLiteTransactionHandler.WriteOp<Void>() {
            @Override
            protected Void doRun(Connection cx) throws IOException, SQLException {
                String sql = format("INSERT OR REPLACE INTO %s (alias, nid) VALUES (?,?)", MAPPINGS);

                try (PreparedStatement ps = cx.prepareStatement(log(sql, LOG, from))) {
                    ps.setString(1, from);
                    ps.setString(2, to);

                    ps.executeUpdate();
                }
                return null;
            }
        });
    }

    @Override
    public String mapping(final String node, DataSource ds) {
        return new DbOp<String>() {
            @Override
            protected String doRun(Connection cx) throws IOException, SQLException {
                String sql = format("SELECT nid FROM %s WHERE alias = ?", MAPPINGS);

                String nid = null;
                try (PreparedStatement ps = cx.prepareStatement(log(sql, LOG, node))) {
                    ps.setString(1, node);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            nid = rs.getString(1);
                        }
                    }
                }
                return nid;
            }
        }.run(ds);
    }

    @Override
    public void property(final String node, final String key, final String val, DataSource ds) {
        runTx(new SQLiteTransactionHandler.WriteOp<Void>() {
            @Override
            protected Void doRun(Connection cx) throws IOException, SQLException {
                String sql = format("INSERT OR REPLACE INTO %s (nid,key,val) VALUES (?,?,?)", PROPS);

                try (PreparedStatement ps = cx.prepareStatement(log(sql, LOG, node, key, val))) {
                    ps.setString(1, node);
                    ps.setString(2, key);
                    ps.setString(3, val);

                    ps.executeUpdate();
                }
                return null;
            }
        });
    }

    @Override
    public String property(final String node, final String key, DataSource ds) {
        return new DbOp<String>() {
            @Override
            protected String doRun(Connection cx) throws IOException, SQLException {
                String sql = format("SELECT val FROM %s WHERE nid = ? AND key = ?", PROPS);

                try (PreparedStatement ps = cx.prepareStatement(log(sql, LOG, node, key))) {
                    ps.setString(1, node);
                    ps.setString(2, key);

                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString(1);
                        } else {
                            return null;
                        }
                    }
                }
            }
        }.run(ds);
    }

    @Override
    public Iterable<String> outgoing(final String node, DataSource ds) {

        final Iterable<String> matches = new DbOp<Iterable<String>>() {
            @Override
            protected Iterable<String> doRun(Connection cx) throws SQLException {
                String sql = format("SELECT dst FROM %s WHERE src = ?", EDGES);
                List<String> matches = new ArrayList<>(2);
                try (PreparedStatement st = cx.prepareStatement(log(sql, LOG))) {
                    st.setString(1, node);
                    try (ResultSet rs = st.executeQuery()) {
                        while (rs.next()) {
                            String id = rs.getString(1);
                            matches.add(id);
                        }
                    }
                }
                return matches;
            }
        }.run(ds);

        return matches;

    }

    @Override
    public Iterable<String> incoming(final String node, DataSource ds) {
        final Iterable<String> matches = new DbOp<Iterable<String>>() {
            @Override
            protected Iterable<String> doRun(Connection cx) throws SQLException {
                String sql = format("SELECT src FROM %s WHERE dst = ?", EDGES);
                List<String> matches = new ArrayList<>(2);
                try (PreparedStatement st = cx.prepareStatement(log(sql, LOG))) {
                    st.setString(1, node);
                    try (ResultSet rs = st.executeQuery()) {
                        while (rs.next()) {
                            String id = rs.getString(1);
                            matches.add(id);
                        }
                    }
                }
                return matches;
            }
        }.run(ds);

        return matches;
    }

    @Override
    public void clear(DataSource ds) {
        runTx(new SQLiteTransactionHandler.WriteOp<Void>() {
            @Override
            protected Void doRun(Connection cx) throws IOException, SQLException {
                try (Statement st = cx.createStatement()) {

                    String sql = format("DELETE FROM %s", PROPS);
                    st.execute(log(sql, LOG));

                    sql = format("DELETE FROM %s", EDGES);
                    st.execute(log(sql, LOG));

                    sql = format("DELETE FROM %s", NODES);
                    st.execute(log(sql, LOG));

                    sql = format("DELETE FROM %s", MAPPINGS);
                    st.execute(log(sql, LOG));
                }
                return null;
            }
        });
    }
}
