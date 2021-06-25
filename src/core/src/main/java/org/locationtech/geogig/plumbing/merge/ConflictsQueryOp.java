/* Copyright (c) 2016 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.plumbing.merge;

import java.util.stream.Stream;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.repository.Conflict;
import org.locationtech.geogig.repository.impl.AbstractGeoGigOp;

import com.google.common.base.Supplier;

/**
 * Internal command to return an iterator of conflicts in the operation's namespace (i.e.
 * transaction space)
 * 
 */
public class ConflictsQueryOp extends AbstractGeoGigOp<Stream<Conflict>>
        implements Supplier<Stream<Conflict>> {

    private String parentPathFilter = null;

    protected @Override Stream<Conflict> _call() {
        if (repository().isOpen()) {
            return conflictsDatabase().getByPrefix(null, parentPathFilter);
        }
        return Stream.empty();
    }

    public ConflictsQueryOp setPrefixFilter(@Nullable String parentPath) {
        this.parentPathFilter = parentPath;
        return this;
    }

    public @Override Stream<Conflict> get() {
        return call();
    }
}
