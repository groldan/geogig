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

import java.io.IOException;

import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;

public class FeatureServiceSource extends ContentFeatureSource {

    private FeatureServiceClient client;

    public FeatureServiceSource(FeatureServiceClient client, ContentEntry entry) {
        super(entry, Query.ALL);
        this.client = client;
    }

    @Override
    protected SimpleFeatureType buildFeatureType() throws IOException {
        Name name = super.getEntry().getName();
        String typeName = name.getLocalPart();
        SimpleFeatureType schema = client.getSchema(typeName);

        return schema;
    }

    @Override
    protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
        ReferencedEnvelope bounds = client.getBounds(query, getSchema());
        return bounds;
    }

    @Override
    protected int getCountInternal(Query query) throws IOException {
        int count = client.getCount(query, getSchema());
        return count;
    }

    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query)
            throws IOException {
        FeatureReader<SimpleFeatureType, SimpleFeature> features = client.getFeatures(query,
                getSchema());
        return features;
    }

}
