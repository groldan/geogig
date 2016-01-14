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
import static org.locationtech.geogig.storage.sqlite.XerialConflictsDatabaseUtil.getCountConflictsSql;
import static org.locationtech.geogig.storage.sqlite.XerialConflictsDatabaseUtil.getInitSql;
import static org.locationtech.geogig.storage.sqlite.XerialConflictsDatabaseUtil.getInsertConflictSql;
import static org.locationtech.geogig.storage.sqlite.XerialConflictsDatabaseUtil.getRemoveConflictsSql;
import static org.locationtech.geogig.storage.sqlite.XerialConflictsDatabaseUtil.getRetrieveConflictsSql;
import static org.locationtech.geogig.storage.sqlite.XerialConflictsDatabaseUtil.setInsertParams;
import static org.locationtech.geogig.storage.sqlite.XerialConflictsDatabaseUtil.setNamespaceParam;
import static org.locationtech.geogig.storage.sqlite.XerialConflictsDatabaseUtil.setRemoveConflictParams;

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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

/**
 * Conflicts database based on Xerial SQLite jdbc driver.
 * 
 * @author Justin Deoliveira, Boundless
 */
class XerialConflictsDatabaseV2 extends SQLiteConflictsDatabase<DataSource> {

    final static Logger LOG = LoggerFactory.getLogger(XerialConflictsDatabaseV2.class);

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
                String sql = getInitSql();

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
                String sql = getCountConflictsSql(namespace);

                try (PreparedStatement ps = cx.prepareStatement(log(sql, LOG, namespace))) {
                    setNamespaceParam(ps, namespace);
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
        Iterable<String> iterableString = new DbOp<Iterable<String>>() {
            @Override
            protected Iterable<String> doRun(Connection cx) throws IOException, SQLException {
                final String sql = getRetrieveConflictsSql(namespace, pathFilter);
                // We need an Iterable<String> here as a sink for the ResultSet
                // because we lose the results once we close the
                // PreparedStatement in the try-with-resources line. Creating a
                // deferred/delayed Iterable with a ResultSet backing it won't
                // work in this case.
                ImmutableList.Builder<String> listBuilder = new ImmutableList.Builder<>();

                try(PreparedStatement ps = cx.prepareStatement(log(sql, LOG, namespace))) {
                    setNamespaceParam(ps, namespace);
                    ResultSet rs = ps.executeQuery();
                    while (rs.next()) {
                        String conflict = rs.getString(1);
                        listBuilder.add(conflict);
                    }
                }
                return listBuilder.build();
            }
        }.run(ds);

        return iterableString;
    }

    @Override
    protected void put(final String namespace, final String path, final String conflict,
            DataSource ds) {

        WriteOp<Void> op = new WriteOp<Void>() {
            @Override
            protected Void doRun(Connection cx) throws IOException, SQLException {
                String sql = getInsertConflictSql();

                log(sql, LOG, namespace, path, conflict);

                cx.setAutoCommit(false);
                try (PreparedStatement ps = cx.prepareStatement(sql)) {
                    setInsertParams(ps, namespace, path, conflict);

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
                String sql = getRemoveConflictsSql(namespace, path);

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
}
