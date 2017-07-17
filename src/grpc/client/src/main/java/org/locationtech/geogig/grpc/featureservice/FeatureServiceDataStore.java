/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.grpc.featureservice;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.List;

import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.NameImpl;
import org.locationtech.geogig.grpc.client.Stubs;
import org.locationtech.geogig.grpc.stream.FeatureServiceGrpc.FeatureServiceBlockingStub;
import org.opengis.feature.type.Name;

import com.google.common.collect.Lists;

public class FeatureServiceDataStore extends ContentDataStore {

    private Stubs stubs;

    private FeatureServiceClient client;

    public FeatureServiceDataStore(Stubs stubs) {
        checkNotNull(stubs);
        this.stubs = stubs;
        FeatureServiceBlockingStub clientStub = stubs.newFeatureServiceBlockingStub();
        this.client = new FeatureServiceClient(clientStub);
    }

    public @Override void dispose() {
        Stubs stubs = this.stubs;
        this.stubs = null;
        if (stubs != null) {
            try {
                super.dispose();
            } finally {
                stubs.close();
            }
        }
    }

    protected @Override List<Name> createTypeNames() throws IOException {
        final String namespaceURI = super.getNamespaceURI();
        List<String> typeNames = client.getTypeNames();
        return Lists.transform(typeNames, (s) -> new NameImpl(namespaceURI, s));
    }

    protected @Override ContentFeatureSource createFeatureSource(ContentEntry entry)
            throws IOException {
        return new FeatureServiceSource(client, entry);
    }

}
