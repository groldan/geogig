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

import static com.google.common.base.Preconditions.checkArgument;
import static org.locationtech.geogig.grpc.common.Constants.SCHEME;

import java.net.URI;
import java.util.List;

import org.locationtech.geogig.grpc.client.ManagedChannelPool;
import org.locationtech.geogig.plumbing.ResolveGeogigURI;
import org.locationtech.geogig.repository.Context;
import org.locationtech.geogig.repository.Hints;
import org.locationtech.geogig.repository.Repository;
import org.locationtech.geogig.repository.RepositoryConnectionException;
import org.locationtech.geogig.repository.RepositoryResolver;
import org.locationtech.geogig.repository.impl.GeoGIG;
import org.locationtech.geogig.repository.impl.GlobalContextBuilder;
import org.locationtech.geogig.storage.ConfigDatabase;

import com.google.common.base.Optional;

import io.grpc.ManagedChannel;

public class GRPCRepositoryResolver extends RepositoryResolver {

    public static final GRPCRepositoryResolver INSTANCE = new GRPCRepositoryResolver();

    public static URI getRPCRepositoryURI(Hints hints) {
        Optional<URI> uri = new ResolveGeogigURI(null, hints).call();
        checkArgument(uri.isPresent(), "Repository URI not provided");

        URI location = uri.get();
        return location;
    }

    public static void checkURI(URI location) {
        checkArgument(SCHEME.equals(location.getScheme()), "Repository URI is not %s://", SCHEME);
        checkArgument(location.getHost() != null, "Host not provided: %s", location);
    }

    @Override
    public boolean canHandle(URI repoURI) {
        return canHandleURIScheme(repoURI.getScheme());
    }

    @Override
    public boolean canHandleURIScheme(String scheme) {
        return SCHEME.equals(scheme);
    }

    @Override
    public boolean repoExists(URI repoURI) throws IllegalArgumentException {
        ManagedChannel channel = ManagedChannelPool.INSTANCE.acquire(repoURI);
        try {
            return true;
        } finally {
            ManagedChannelPool.INSTANCE.release(channel);
        }
    }

    @Override
    public URI buildRepoURI(URI rootRepoURI, String repoName) {
        URI repoUri = URI.create(rootRepoURI.toString() + "/" + repoName);
        return repoUri;
    }

    @Override
    public List<String> listRepoNamesUnderRootURI(URI rootRepoURI) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void initialize(URI repoURI, Context repoContext) throws IllegalArgumentException {
        throw new UnsupportedOperationException("initialize not supported");
    }

    @Override
    public ConfigDatabase getConfigDatabase(URI repoURI, Context repoContext, boolean rootUri) {
        checkURI(repoURI);
        Hints hints = new Hints().uri(repoURI);
        return new ConfigDatabaseClient(hints);
    }

    @Override
    public Repository open(URI repositoryLocation) throws RepositoryConnectionException {
        try {
            checkURI(repositoryLocation);
        } catch (Exception e) {
            throw new RepositoryConnectionException(e.getMessage());
        }

        Hints hints = new Hints();
        hints.set(Hints.REPOSITORY_URL, repositoryLocation.toString());
        Context context = GlobalContextBuilder.builder().build(hints);
        Repository repository = new GeoGIG(context).getRepository();
        repository.open();
        return repository;
    }

    @Override
    public String getName(URI repoURI) {
        String rawPath = repoURI.getRawPath();
        int idx = rawPath.lastIndexOf('/');
        String name = rawPath.substring(idx + 1);
        return name;
    }

    @Override
    public boolean delete(URI repositoryLocation) throws Exception {
        throw new UnsupportedOperationException("not yet implemented");
    }

}
