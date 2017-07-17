/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.grpc.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.locationtech.geogig.model.RevObject;
import org.locationtech.geogig.model.impl.RevFeatureBuilder;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;

public final class BlockingQueueIterator<T extends RevObject> extends AbstractIterator<T> {

    private static final RevObject TERMINATING_TOKEN = RevFeatureBuilder.builder().build();

    private static final RevObject ABORTING_TOKEN = RevFeatureBuilder.builder().build();

    private BlockingQueue<T> queue;

    private Iterator<T> buff = Collections.emptyIterator();

    private Throwable abortCause;

    public BlockingQueueIterator() {
        this.queue = new LinkedBlockingQueue<>();
    }

    @Override
    protected T computeNext() {
        T obj = tryComputeNext();
        if (obj == TERMINATING_TOKEN) {
            return endOfData();
        }
        if (obj == ABORTING_TOKEN) {
            throw Throwables.propagate(abortCause);
        }
        return obj;
    }

    private T tryComputeNext() {
        if (buff.hasNext()) {
            T next = buff.next();
            return next;
        }
        while (queue.isEmpty()) {
            try {
                T obj = queue.poll(100, TimeUnit.MILLISECONDS);
                if (obj != null) {
                    return obj;
                }
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            }
        }
        List<T> tmp = new ArrayList<>();
        queue.drainTo(tmp);
        buff = tmp.iterator();
        return computeNext();
    }

    public void offer(T obj) {
        Preconditions.checkState(queue.offer(obj));
    }

    @SuppressWarnings("unchecked")
    public void completed() {
        offer((T) TERMINATING_TOKEN);
    }

    @SuppressWarnings("unchecked")
    public void abort(Throwable cause) {
        this.abortCause = cause;
        queue.clear();
        queue.offer((T) ABORTING_TOKEN);
    }

}