/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.grpc.repository;

import org.locationtech.geogig.storage.StorageProvider;
import org.locationtech.geogig.storage.VersionedFormat;

public class GRPCStorageProvider extends StorageProvider {

    /**
     * Format name used for configuration.
     */
    public static final String FORMAT_NAME = "grpc";

    /**
     * Implementation version.
     */
    public static final String VERSION = "1";

    static final VersionedFormat GRAPH = new VersionedFormat(FORMAT_NAME, VERSION,
            GraphDatabaseClient.class);

    static final VersionedFormat REFS = new VersionedFormat(FORMAT_NAME, VERSION,
            RefDatabaseClient.class);;

    static final VersionedFormat OBJECTS = new VersionedFormat(FORMAT_NAME, VERSION,
            ObjectDatabaseClient.class);

    static final VersionedFormat INDEX = new VersionedFormat(FORMAT_NAME, VERSION,
            IndexDatabaseClient.class);

    @Override
    public String getName() {
        return FORMAT_NAME;
    }

    @Override
    public String getVersion() {
        return VERSION;
    }

    @Override
    public String getDescription() {
        return "GRPC backend store";
    }

    @Override
    public VersionedFormat getObjectDatabaseFormat() {
        return OBJECTS;
    }

    @Override
    public VersionedFormat getGraphDatabaseFormat() {
        return GRAPH;
    }

    @Override
    public VersionedFormat getRefsDatabaseFormat() {
        return REFS;
    }

    @Override
    public VersionedFormat getIndexDatabaseFormat() {
        return INDEX;
    }
}
