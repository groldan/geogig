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

import static java.lang.String.format;
import static org.locationtech.geogig.storage.sqlite.Xerial.log;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.locationtech.geogig.storage.sqlite.SQLiteTransactionHandler.WriteOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

/**
 * Conflicts database based on Xerial SQLite jdbc driver.
 * 
 * @author Justin Deoliveira, Boundless
 */
class XerialConflictsDatabaseV2 extends SQLiteConflictsDatabase<DataSource> {

    final static Logger LOG = LoggerFactory.getLogger(XerialConflictsDatabaseV2.class);

    final static String CONFLICTS = "conflicts";

    /**
     * SQL statements.
     */
    private static final String GET_CONFLICTS_WITH_NAMESPACE_AND_PATH =
        "SELECT conflict FROM %s WHERE namespace = ? AND path LIKE '%%%s%%'";
    private static final String GET_CONFLICTS_WITH_NAMESPACE =
        "SELECT conflict FROM %s WHERE namespace = ?";
    private static final String GET_CONFLICTS_WITH_PATH =
        "SELECT conflict FROM %s WHERE path LIKE '%%%s%%'";
    private static final String GET_CONFLICTS_NO_NAMESPACE_OR_PATH =
        "SELECT conflict FROM %s";
    private static final String REMOVE_CONFLICTS_WITH_NAMESAPCE_AND_PATH =
        "DELETE FROM %s WHERE namespace = ? AND path = ?";
    private static final String REMOVE_CONFLICTS_WITH_NAMESPACE =
        "DELETE FROM %s WHERE namespace = ?";
    private static final String REMOVE_CONFLICTS_WITH_PATH =
        "DELETE FROM %s WHERE path = ?";
    private static final String REMOVE_CONFLICTS_NO_NAMESPACE_OR_PATH =
        "DELETE FROM %s";

    private SQLiteTransactionHandler txHandler;

    @Inject
    public XerialConflictsDatabaseV2(DataSource ds, SQLiteTransactionHandler txHandler) {
        super(ds);
        this.txHandler = txHandler;
    }

    @Override
    protected void init(DataSource ds) {
        WriteOp<Void> op = new WriteOp<Void>() {
            @Override
            protected Void doRun(Connection cx) throws SQLException {
                String sql = format("CREATE TABLE IF NOT EXISTS %s (namespace VARCHAR, "
                        + "path VARCHAR, conflict VARCHAR, PRIMARY KEY(namespace,path))", CONFLICTS);

                cx.setAutoCommit(false);
                try (Statement statement = cx.createStatement()) {
                    statement.execute(log(sql, LOG));
                    cx.commit();
                } catch (SQLException e) {
                    cx.rollback();
                    throw e;
                }

                return null;
            }
        };
        txHandler.runTx(op);
    }

    @Override
    protected int count(final String namespace, DataSource ds) {
        Integer count = new DbOp<Integer>() {
            @Override
            protected Integer doRun(Connection cx) throws IOException, SQLException {
                String sql = format("SELECT count(*) FROM %s WHERE namespace = ?", CONFLICTS);

                try (PreparedStatement ps = cx.prepareStatement(log(sql, LOG, namespace))) {
                    ps.setString(1, namespace);
                    int count = 0;
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            count = rs.getInt(1);
                        }
                    }
                    return Integer.valueOf(count);
                }
            }
        }.run(ds);

        return count.intValue();
    }

    @Override
    protected Iterable<String> get(final String namespace, final String pathFilter, DataSource ds) {
        Connection cx = Xerial.newConnection(ds);
        ResultSet rs = new DbOp<ResultSet>() {
            @Override
            protected ResultSet doRun(Connection cx) throws IOException, SQLException {
                String sql = buildGetConflictsSql(namespace, pathFilter);

                PreparedStatement ps = cx.prepareStatement(log(sql, LOG, namespace));
                if (null != namespace) {
                    ps.setString(1, namespace);
                }

                return ps.executeQuery();
            }
        }.run(cx);

        return new StringResultSetIterable(rs, cx);
    }

    @Override
    protected void put(final String namespace, final String path, final String conflict,
            DataSource ds) {

        WriteOp<Void> op = new WriteOp<Void>() {
            @Override
            protected Void doRun(Connection cx) throws IOException, SQLException {
                String sql = format("INSERT OR REPLACE INTO %s VALUES (?,?,?)", CONFLICTS);

                log(sql, LOG, namespace, path, conflict);

                cx.setAutoCommit(false);
                try (PreparedStatement ps = cx.prepareStatement(sql)) {
                    ps.setString(1, namespace);
                    ps.setString(2, path);
                    ps.setString(3, conflict);

                    ps.executeUpdate();
                    cx.commit();
                } catch (SQLException e) {
                    cx.rollback();
                    throw e;
                }
                return null;
            }
        };
        txHandler.runTx(op);
    }

    @Override
    protected void remove(final String namespace, final String path, DataSource ds) {
        WriteOp<Void> op = new WriteOp<Void>() {
            @Override
            protected Void doRun(Connection cx) throws IOException, SQLException {
                String sql = buildRemoveConflictsSql(namespace, path);

                log(sql, LOG, namespace, path);

                cx.setAutoCommit(false);
                try (PreparedStatement ps = cx.prepareStatement(sql)) {
                    // put the parameterized values in, if we have them
                    setRemoveConflictParams(ps, namespace, path);
                    ps.executeUpdate();
                    cx.commit();
                } catch (SQLException e) {
                    cx.rollback();
                    throw e;
                }
                return null;
            }
        };
        txHandler.runTx(op);
    }

    private String buildGetConflictsSql(final String namespace, final String pathFilter) {
        // need to build a select statement with the correct WHERE clause, if namespace and/or
        // pathFilter are not NULL.
        if (null != namespace) {
            // we have a namespace, do we have a pathFilter?
            if (null != pathFilter) {
                // we have both
                return format(XerialConflictsDatabaseV2.GET_CONFLICTS_WITH_NAMESPACE_AND_PATH,
                    XerialConflictsDatabaseV2.CONFLICTS, pathFilter);
            }
            // we have namespace but no pathFilter
            return format(XerialConflictsDatabaseV2.GET_CONFLICTS_WITH_NAMESPACE,
                XerialConflictsDatabaseV2.CONFLICTS);
        }
        // we have no namespace, do we have a pathFilter?
        if (null != pathFilter) {
            // we have a pathFilter
            return format(XerialConflictsDatabaseV2.GET_CONFLICTS_WITH_PATH,
                XerialConflictsDatabaseV2.CONFLICTS, pathFilter);
        }
        // we have neither namespace, nor pathfilter
        return format(XerialConflictsDatabaseV2.GET_CONFLICTS_NO_NAMESPACE_OR_PATH,
            XerialConflictsDatabaseV2.CONFLICTS);
    }

    private String buildRemoveConflictsSql(final String namespace, final String path) {
        if (null != namespace) {
            // we have a namespace, do we have a path?
            if (null != path) {
                // we have both
                return format(XerialConflictsDatabaseV2.REMOVE_CONFLICTS_WITH_NAMESAPCE_AND_PATH,
                    XerialConflictsDatabaseV2.CONFLICTS);
            }
            // we have a namespace but no path
            return format(XerialConflictsDatabaseV2.REMOVE_CONFLICTS_WITH_NAMESPACE,
                XerialConflictsDatabaseV2.CONFLICTS);
        }
        // we have no namespace, see if we have a path
        if (null != path) {
            // we have a path but no namespace
            return format(XerialConflictsDatabaseV2.REMOVE_CONFLICTS_WITH_PATH,
                XerialConflictsDatabaseV2.CONFLICTS);
        }
        // we have no namespace or path
        return format(XerialConflictsDatabaseV2.REMOVE_CONFLICTS_NO_NAMESPACE_OR_PATH,
            XerialConflictsDatabaseV2.CONFLICTS);
    }

    private void setRemoveConflictParams(final PreparedStatement ps, final String namespace,
        final String path) throws SQLException {
        if (null != namespace) {
            // we have a namespace, it's always index 1 if we have it
            ps.setString(1, namespace);
            if (null != path) {
                // we have path too, it's index 2
                ps.setString(2, path);
            }
        } else {
            // no namespace, do we have a path?
            if (null != path) {
                // we have only path, put it in index 1
                ps.setString(1, path);
            }
        }
    }
}
