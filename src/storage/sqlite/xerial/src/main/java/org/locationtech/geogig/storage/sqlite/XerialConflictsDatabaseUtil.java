/* Copyright (c) 2015 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Erik Merkle (Boundless) - initial implementation
 */
package org.locationtech.geogig.storage.sqlite;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.eclipse.jdt.annotation.NonNull;

/**
 * Utility class for managing Xerial DB interaction for Conflicts.
 */
class XerialConflictsDatabaseUtil {

    /**
     * Conflicts table name.
     */
    static final String CONFLICTS = "conflicts";

    /**
     * SQL statements.
     */
    private static final String GET_CONFLICTS_WITH_NAMESPACE_AND_PATH =
        "SELECT conflict FROM %s WHERE namespace = ? AND path LIKE '%%%s%%'";
    private static final String GET_CONFLICTS_WITH_NAMESPACE =
        String.format("SELECT conflict FROM %s WHERE namespace = ?", CONFLICTS);
    private static final String GET_CONFLICTS_WITH_PATH =
        "SELECT conflict FROM %s WHERE path LIKE '%%%s%%'";
    private static final String GET_CONFLICTS_NO_NAMESPACE_OR_PATH =
        String.format("SELECT conflict FROM %s", CONFLICTS);
    private static final String REMOVE_CONFLICTS_WITH_NAMESAPCE_AND_PATH =
        String.format("DELETE FROM %s WHERE namespace = ? AND path = ?", CONFLICTS);
    private static final String REMOVE_CONFLICTS_WITH_NAMESPACE =
        String.format("DELETE FROM %s WHERE namespace = ?", CONFLICTS);
    private static final String REMOVE_CONFLICTS_WITH_PATH =
        String.format("DELETE FROM %s WHERE path = ?", CONFLICTS);
    private static final String REMOVE_CONFLICTS_NO_NAMESPACE_OR_PATH =
        String.format("DELETE FROM %s", CONFLICTS);
    private static final String COUNT_CONFLICTS_WITH_NAMESPACE =
        String.format("SELECT count(*) FROM %s WHERE namespace = ?", CONFLICTS);
    private static final String COUNT_CONFLICTS_NO_NAMESPACE =
        String.format("SELECT count(*) FROM %s", CONFLICTS);
    private static final String CREATE_TABLE =
        String.format("CREATE TABLE IF NOT EXISTS %s (namespace VARCHAR, path VARCHAR, " +
            "conflict VARCHAR, PRIMARY KEY(namespace,path))", CONFLICTS);
    private static final String INSERT_CONFLICT =
        String.format("INSERT OR REPLACE INTO %s VALUES (?,?,?)", CONFLICTS);

    static String getCountConflictsSql(final String namespace) {
        if (null != namespace) {
            // we have a namespcae
            return XerialConflictsDatabaseUtil.COUNT_CONFLICTS_WITH_NAMESPACE;
        }
        // no namespace
        return XerialConflictsDatabaseUtil.COUNT_CONFLICTS_NO_NAMESPACE;
    }

    static String getRetrieveConflictsSql(final String namespace, final String pathFilter) {
        // need to build a select statement with the correct WHERE clause, if namespace and/or
        // pathFilter are not NULL.
        if (null != namespace) {
            // we have a namespace, do we have a pathFilter?
            if (null != pathFilter) {
                // we have both
                return String.format(
                    XerialConflictsDatabaseUtil.GET_CONFLICTS_WITH_NAMESPACE_AND_PATH,
                    XerialConflictsDatabaseUtil.CONFLICTS, pathFilter);
            }
            // we have namespace but no pathFilter
            return XerialConflictsDatabaseUtil.GET_CONFLICTS_WITH_NAMESPACE;
        }
        // we have no namespace, do we have a pathFilter?
        if (null != pathFilter && !"null".equals(pathFilter)) {
            // we have a pathFilter
            return String.format(XerialConflictsDatabaseUtil.GET_CONFLICTS_WITH_PATH,
                XerialConflictsDatabaseUtil.CONFLICTS, pathFilter);
        }
        // we have neither namespace, nor pathfilter
        return XerialConflictsDatabaseUtil.GET_CONFLICTS_NO_NAMESPACE_OR_PATH;
    }

    static String getRemoveConflictsSql(final String namespace, final String path) {
        if (null != namespace) {
            // we have a namespace, do we have a path?
            if (null != path) {
                // we have both
                return XerialConflictsDatabaseUtil.REMOVE_CONFLICTS_WITH_NAMESAPCE_AND_PATH;
            }
            // we have a namespace but no path
            return XerialConflictsDatabaseUtil.REMOVE_CONFLICTS_WITH_NAMESPACE;
        }
        // we have no namespace, see if we have a path
        if (null != path && !"null".equals(path)) {
            // we have a path but no namespace
            return XerialConflictsDatabaseUtil.REMOVE_CONFLICTS_WITH_PATH;
        }
        // we have no namespace or path
        return XerialConflictsDatabaseUtil.REMOVE_CONFLICTS_NO_NAMESPACE_OR_PATH;
    }

    static String getInsertConflictSql() {
        return XerialConflictsDatabaseUtil.INSERT_CONFLICT;
    }

    static String getInitSql() {
        return XerialConflictsDatabaseUtil.CREATE_TABLE;
    }

    static void setRemoveConflictParams(final PreparedStatement ps, final String namespace,
        final String path) throws SQLException {
        if (null != namespace) {
            // we have a namespace, it's always index 1 if we have it
            ps.setString(1, namespace);
            if (null != path) {
                // we have path too, it's index 2
                ps.setString(2, path);
            }
        }
        else if (null != path && !"null".equals(path)) {
            // we have only path, put it in index 1
            ps.setString(1, path);
        }
    }

    static void setNamespaceParam(final PreparedStatement ps, final String namespace)
        throws SQLException {

        if (null != namespace) {
            ps.setString(1, namespace);
        }
    }

    static void setInsertParams(final PreparedStatement ps, @NonNull final String namespace,
        @NonNull final String path, @NonNull final String conflict) throws SQLException {
        
        ps.setString(1, namespace);
        ps.setString(2, path);
        ps.setString(3, conflict);
    }
}
