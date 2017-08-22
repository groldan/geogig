/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package benchmarks.states;

import java.net.URI;
import java.util.Map;

import org.locationtech.geogig.cli.CLIContextBuilder;
import org.locationtech.geogig.repository.AbstractGeoGigOp;
import org.locationtech.geogig.repository.Repository;
import org.locationtech.geogig.repository.RepositoryConnectionException;
import org.locationtech.geogig.repository.RepositoryResolver;
import org.locationtech.geogig.repository.impl.GlobalContextBuilder;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.locationtech.geogig.storage.cache.CacheManager;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;

@State(Scope.Benchmark)
public class RepoState {

    private Map<String, String> templates;

    @Param({ //
            "several_commits_no_changes" //
            , //
            "few_commits_small_changesets" //
            , //
            "few_commits_large_data" //
            , //
            "several_commits_small_changesets"//
    })
    public String repoName;

    @Param({ //
            "rocksdb"//
////            , //
//            "postgres"//
    })
    String storageType;

    private URI repoURI;

    private Repository repo;

    public @Setup void initialize(final ExternalResources resources)
            throws RepositoryConnectionException {

        String rocksURITemplate = resources.getRocksURITemplate();
        String pgURITemplate = resources.getPostgresURITemplate();
        templates = ImmutableMap.of("rocksdb", rocksURITemplate, "postgres", pgURITemplate);

        GlobalContextBuilder.builder(new CLIContextBuilder());
        repoURI = buildURI();
        repo = RepositoryResolver.load(repoURI);
    }

    public @Override String toString() {
        return String.valueOf(repoURI);
    }

    private URI buildURI() {
        String uriTemplate = templates.get(storageType);
        String uri = uriTemplate.replace("${repo}", repoName);
        return URI.create(uri);
    }

    public @TearDown void destroy() {
        if (repo != null) {
            repo.close();
            repo = null;
        }
    }

    public URI getRepoURI() {
        Preconditions.checkNotNull(repoURI);
        return repoURI;
    }

    /**
     * Closes and re-opens the repository to discard cached data
     */
    public void reset() {
        repo.close();
        try {
            repo = RepositoryResolver.load(repoURI);
        } catch (RepositoryConnectionException e) {
            Throwables.propagate(e);
        }
    }

    public Repository repo() {
        Preconditions.checkNotNull(repo);
        return repo;
    }

    public <T extends AbstractGeoGigOp<?>> T command(Class<T> commandClass) {
        return repo().command(commandClass);
    }

    public ObjectDatabase objectDatabase() {
        return repo().objectDatabase();
    }

    public void clearCaches() {

        CacheManager cacheManager = CacheManager.INSTANCE;
        long sizePre = cacheManager.getSize();
        if (sizePre > 0) {
            cacheManager.clear();
            long sizePost = cacheManager.getSize();
            System.err.printf("Cleared %,d objects from cache\n", (sizePre - sizePost));
        }
    }

}
