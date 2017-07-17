/* Copyright (c) 2016 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.web;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.locationtech.geogig.rest.repository.InitCommandResource.INIT_CMD;
import static org.locationtech.geogig.web.api.RESTUtils.getStringAttribute;

import java.io.Serializable;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;

import org.locationtech.geogig.repository.Hints;
import org.locationtech.geogig.repository.Repository;
import org.locationtech.geogig.repository.RepositoryConnectionException;
import org.locationtech.geogig.repository.RepositoryResolver;
import org.locationtech.geogig.repository.impl.GeoGIG;
import org.locationtech.geogig.repository.impl.GlobalContextBuilder;
import org.locationtech.geogig.repository.impl.MultiRepositoryResolver;
import org.locationtech.geogig.rest.repository.InitRequestUtil;
import org.locationtech.geogig.rest.repository.RepositoryProvider;
import org.restlet.data.Method;
import org.restlet.data.Request;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;

/**
 * {@link RepositoryProvider} that looks up the corresponding {@link GeoGIG} instance to a given
 * {@link Request} by asking the geoserver's {@link RepositoryManager}
 */
public class MultiRepositoryProvider implements RepositoryProvider {

    private MultiRepositoryResolver repositories;

    public MultiRepositoryProvider(final URI rootRepoURI) {
        checkNotNull(rootRepoURI, "root repo URI is null");
        repositories = new MultiRepositoryResolver(rootRepoURI);
    }

    @Override
    public Iterator<String> findRepositories() {
        return repositories.findRepositories().iterator();
    }

    private boolean isInitRequest(Request request) {
        // if the request is a PUT, and the request path ends in "init", it's an INIT request.
        if (Method.PUT.equals(request.getMethod())) {
            Map<String, Object> attributes = request.getAttributes();
            if (attributes != null && attributes.containsKey("command")) {
                return INIT_CMD.equals(attributes.get("command"));
            } else if (request.getResourceRef() != null) {
                String path = request.getResourceRef().getPath();
                return path != null && path.contains(INIT_CMD);
            }
        }
        return false;
    }

    @Override
    public Optional<Repository> getGeogig(Request request) {
        final String repositoryName = getStringAttribute(request, "repository");
        if (null == repositoryName) {
            return Optional.absent();
        }
        if (isInitRequest(request)) {
            // init request, get a GeoGig repo based on the request
            Optional<Repository> initRepo = InitRequestHandler.createGeoGIG(request);
            if (initRepo.isPresent()) {
                // init request was sufficient
                return initRepo;
            }
        }
        return Optional.of(getGeogig(repositoryName));
    }

    public Repository getGeogig(final String repositoryName) {
        try {
            return repositories.getRepository(repositoryName);
        } catch (RepositoryConnectionException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void delete(Request request) {
        final String repositoryName = getStringAttribute(request, "repository");
        try {
            repositories.delete(repositoryName);
        } catch (RepositoryConnectionException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void invalidate(String repoName) {
        this.repositories.invalidate(repoName);
    }
    
    public void invalidateAll() {
        this.repositories.invalidateAll();
    }

    private static class InitRequestHandler {

        private static Optional<Repository> createGeoGIG(Request request) {
            try {
                final Hints hints = InitRequestUtil.createHintsFromRequest(request);
                final Optional<Serializable> repositoryUri = hints.get(Hints.REPOSITORY_URL);
                if (!repositoryUri.isPresent()) {
                    // didn't successfully build a Repository URI
                    return Optional.absent();
                }
                final URI repoUri = URI.create(repositoryUri.get().toString());
                final RepositoryResolver resolver = RepositoryResolver.lookup(repoUri);
                final Repository repository = GlobalContextBuilder.builder().build(hints)
                        .repository();
                if (resolver.repoExists(repoUri)) {
                    // open it
                    repository.open();
                }
                // now build the repo with the Hints
                return Optional.fromNullable(repository);
            } catch (Exception ex) {
                Throwables.propagate(ex);
            }
            return Optional.absent();
        }
    }
}