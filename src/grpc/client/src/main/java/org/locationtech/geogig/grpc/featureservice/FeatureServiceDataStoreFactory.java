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

import java.awt.RenderingHints.Key;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.Collections;
import java.util.Map;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;
import org.geotools.data.DataUtilities;
import org.locationtech.geogig.grpc.client.Stubs;
import org.locationtech.geogig.grpc.common.Constants;

import com.google.common.base.Preconditions;

public class FeatureServiceDataStoreFactory implements DataStoreFactorySpi {

    public static final Integer DEFAULT_PORT = Constants.DEFAULT_PORT;
    
    public static final Param HOST = new Param("geogiggrpc.host", String.class,
            "GRPC Server host name or IP address", true, "localhost");

    public static final Param PORT = new Param("geogiggrpc.port", Integer.class, "GRPC server port",
            false, DEFAULT_PORT);

    public static final Param REPOSITORY = new Param("geogiggrpc.repository", String.class,
            "Repository name", true, "<repository name>");

    public static final Param DEFAULT_NAMESPACE = new Param("namespace", String.class,
            "Optional namespace for feature types", false);

    @Override
    public String getDisplayName() {
        return "GeoGig GRPC";
    }

    @Override
    public String getDescription() {
        return "Fetches data from a geogig GRPC server";
    }

    @Override
    public Param[] getParametersInfo() {
        return new Param[] {DEFAULT_NAMESPACE, HOST, PORT, REPOSITORY };
    }

    @Override
    public boolean canProcess(Map<String, Serializable> params) {
        boolean canProcess = DataUtilities.canProcess(params, getParametersInfo());
        return canProcess;
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public Map<Key, ?> getImplementationHints() {
        return Collections.emptyMap();
    }

    @Override
    public FeatureServiceDataStore createDataStore(Map<String, Serializable> params)
            throws IOException {

        Preconditions.checkArgument(canProcess(params), "Can't process the given parameters");
        String host = (String) HOST.lookUp(params);
        Integer port = (Integer) PORT.lookUp(params);
        String reposiroty = (String) REPOSITORY.lookUp(params);
        String defaultNamespace = (String) DEFAULT_NAMESPACE.lookUp(params);
        if (port == null) {
            port = Constants.DEFAULT_PORT;
        }
        URI repoURI = URI.create("grpc://" + host + ":" + port + "/" + reposiroty);
        Stubs stubs = Stubs.forURI(repoURI);
        stubs.open();
        FeatureServiceDataStore dataStore = new FeatureServiceDataStore(stubs);
        if (defaultNamespace != null) {
            dataStore.setNamespaceURI(defaultNamespace);
        }
        return dataStore;
    }

    @Override
    public DataStore createNewDataStore(Map<String, Serializable> params) throws IOException {
        throw new UnsupportedOperationException();
    }

}
