/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.grpc.client;

import org.eclipse.jdt.annotation.Nullable;

import com.google.common.base.Throwables;
import com.google.protobuf.Empty;

import io.grpc.stub.StreamObserver;

public abstract class BasicObserver<T> implements StreamObserver<T> {

    private boolean completed;

    private Throwable error;

    public boolean isCompleted() {
        return completed;
    }

    public boolean isErrored() {
        return error != null;
    }

    public @Nullable Throwable getError() {
        return error;
    }

    public void rethrow() throws RuntimeException {
        if (isErrored()) {
            Throwables.propagate(error);
        }
    }

    @Override
    public void onError(Throwable t) {
        // System.err.println("onError: " + t);
        error = t;
        onCompleted();
    }

    @Override
    public void onCompleted() {
        // System.err.println("onCompleted");
        completed = true;
    }

    public static BasicObserver<Empty> newEmptyObserver() {
        return new BasicObserver<Empty>() {
            @Override
            public void onNext(Empty value) {
                //
            }
        };
    }

    public void awaitCompleteness() throws RuntimeException {
        while (!isCompleted()) {
            // System.err.println("Waiting for complete at " + Thread.currentThread().getName());
            Thread.yield();
        }
        rethrow();
    }
}
