/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.repository.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.locationtech.geogig.plumbing.ResolveGeogigURI;
import org.locationtech.geogig.repository.Context;
import org.locationtech.geogig.repository.Hints;
import org.locationtech.geogig.repository.Repository;
import org.locationtech.geogig.repository.RepositoryConnectionException;
import org.locationtech.geogig.repository.RepositoryResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 */
public class MultiRepositoryResolver {

    private static final Logger LOG = LoggerFactory.getLogger(MultiRepositoryResolver.class);

    private LoadingCache<String, Repository> repositories;

    private final URI rootRepoURI;

    private final RepositoryResolver resolver;

    public MultiRepositoryResolver(final URI rootRepoURI) {
        checkNotNull(rootRepoURI, "root repo URI is null");

        resolver = RepositoryResolver.lookup(rootRepoURI);

        this.rootRepoURI = rootRepoURI;

        try {
            this.repositories = buildCache();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public List<String> findRepositories() {
        return resolver.listRepoNamesUnderRootURI(rootRepoURI);
    }

    public Repository getRepository(final String repositoryName)
            throws RepositoryConnectionException {
        checkNotNull(repositoryName, "provided repository name is null");
        try {
            Repository repository = repositories.get(repositoryName);
            return repository;
        } catch (ExecutionException e) {
            LOG.warn("Unable to load repository {}", repositoryName, e);
            Throwable cause = e.getCause() == null ? e : e.getCause();
            throw new RepositoryConnectionException("Unable to acquire " + repositoryName, cause);
        }
    }

    private static final RemovalListener<String, Repository> removalListener = new RemovalListener<String, Repository>() {

        public void onRemoval(RemovalNotification<String, Repository> notification) {
            final RemovalCause cause = notification.getCause();
            final String repositoryName = notification.getKey();
            final Repository repo = notification.getValue();
            LOG.info("Disposing repository {}. Cause: " + cause(cause));
            try {
                if (repo != null) {
                    repo.close();
                }
            } catch (RuntimeException e) {
                LOG.warn("Error closing repository {}", repositoryName, e);
            }
        }

        private String cause(RemovalCause cause) {
            switch (cause) {
            case COLLECTED:
                return "removed automatically because its key or value was garbage-collected";
            case EXPIRED:
                return "expiration timestamp has passed";
            case EXPLICIT:
                return "manually removed by remove() or invalidateAll()";
            case REPLACED:
                return "manually replaced";
            case SIZE:
                return "evicted due to cache size constraints";
            default:
                return "Unknown";
            }
        }
    };

    private LoadingCache<String, Repository> buildCache() throws IOException {

        CacheLoader<String, Repository> loader = new CacheLoader<String, Repository>() {

            public Repository load(final String repoName) throws Exception {
                Repository repo = loadRepository(repoName);
                return repo;
            }

        };

        LoadingCache<String, Repository> cache = CacheBuilder.newBuilder()//
                .concurrencyLevel(1)//
                .expireAfterAccess(1, TimeUnit.MINUTES)//
                .maximumSize(1024)//
                .removalListener(removalListener)//
                .build(loader);

        return cache;
    }

    @VisibleForTesting
    Repository loadRepository(final String repoName) {
        LOG.info("Loading repository " + repoName);
        Hints hints = new Hints();
        final URI repoURI = resolver.buildRepoURI(rootRepoURI, repoName);
        hints.set(Hints.REPOSITORY_URL, repoURI);
        hints.set(Hints.REPOSITORY_NAME, repoName);

        Context context = GlobalContextBuilder.builder().build(hints);

        Repository repository = context.repository();

        if (!repository.isOpen()) {
            // Only open it if is was an existing repository.
            for (String existingRepo : resolver.listRepoNamesUnderRootURI(rootRepoURI)) {
                if (existingRepo.equals(repoName)) {
                    try {
                        repository.open();
                    } catch (RepositoryConnectionException e) {
                        throw Throwables.propagate(e);
                    }
                    break;
                }
            }
        }

        return repository;
    }

    public void delete(final String repositoryName) throws RepositoryConnectionException {
        Repository repo = getRepository(repositoryName);
        Optional<URI> repoUri = repo.command(ResolveGeogigURI.class).call();
        Preconditions.checkState(repoUri.isPresent(), "No repository to delete.");

        repo.close();
        try {
            GeoGIG.delete(repoUri.get());
            this.repositories.invalidate(repositoryName);
        } catch (Exception e) {
            Throwables.propagate(e);
        }
    }

    public void invalidate(String repoName) {
        this.repositories.invalidate(repoName);
    }

    public void invalidateAll() {
        this.repositories.invalidateAll();
    }

    public static MultiRepositoryResolver of(Repository repository) {
        MultiRepositoryResolver mrr = new MultiRepositoryResolver(repository.getLocation());

        return mrr;
    }
}