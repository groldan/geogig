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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import com.google.common.base.Throwables;

class SQLiteTransactionHandler {

    public static abstract class WriteOp<T> extends DbOp<T> {

        // just a marker interface

    }

    private ExecutorService writerThread;

    private Connection writerConnection;

    private AtomicInteger transactionDepth = new AtomicInteger();

    public SQLiteTransactionHandler(DataSource connPool) {
        try {
            this.writerConnection = connPool.getConnection();
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
        this.writerThread = Executors.newSingleThreadExecutor();

    }

    public synchronized void close() {
        try {
            ExecutorService writerThread = this.writerThread;
            this.writerThread = null;
            if (writerThread != null) {
                writerThread.shutdown();
                try {
                    writerThread.awaitTermination(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } finally {
            Connection writerConnection = this.writerConnection;
            this.writerConnection = null;
            if (writerConnection != null) {
                try {
                    writerConnection.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }
    }

    public synchronized void startTransaction() {
        int depth = transactionDepth.incrementAndGet();
        if (depth == 1) {
            try {
                // System.err.println("BEGIN TRANSACTION........ " +
                // Thread.currentThread().getName());
                // int i = 0;
                // for (StackTraceElement e : Thread.currentThread().getStackTrace()) {
                // System.err.println(e);
                // if (i++ == 10) {
                // break;
                // }
                // }
                writerConnection.setAutoCommit(false);
            } catch (SQLException e) {
                transactionDepth.decrementAndGet();
                throw Throwables.propagate(e);
            }
        }
    }

    public synchronized void endTransaction() {
        int depth = transactionDepth.decrementAndGet();
        if (depth == 0) {
            try {
                // System.err.println("END TRANSACTION........");
                // int i = 0;
                // for (StackTraceElement e : Thread.currentThread().getStackTrace()) {
                // System.err.println(e);
                // if (i++ == 10) {
                // break;
                // }
                // }

                //Stopwatch sw = Stopwatch.createStarted();
                writerConnection.commit();
                //System.err.println("committed in " + sw.stop());
                writerConnection.setAutoCommit(true);
            } catch (SQLException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    public <T> T runTx(final SQLiteTransactionHandler.WriteOp<T> dbop) {

        Callable<T> task = new Callable<T>() {

            @Override
            public T call() throws Exception {
                startTransaction();
                T result;
                try {
                    result = dbop.doRun(writerConnection);
                } catch (Exception e) {
                    transactionDepth.decrementAndGet();
                    writerConnection.rollback();
                    throw e;
                }
                // op succeeded
                endTransaction();
                return result;
            }
        };

        Future<T> future = writerThread.submit(task);
        T result;
        try {
            result = future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw Throwables.propagate(Throwables.getRootCause(e));
        }
        return result;
    }

}
