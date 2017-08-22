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

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

import org.locationtech.geogig.cli.CLIContextBuilder;
import org.locationtech.geogig.porcelain.ConfigOp;
import org.locationtech.geogig.porcelain.ConfigOp.ConfigAction;
import org.locationtech.geogig.porcelain.InitOp;
import org.locationtech.geogig.repository.Context;
import org.locationtech.geogig.repository.Hints;
import org.locationtech.geogig.repository.Repository;
import org.locationtech.geogig.repository.RepositoryResolver;
import org.locationtech.geogig.repository.impl.GlobalContextBuilder;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import com.google.common.base.Preconditions;

/**
 * re A state to provide a ready to use {@link Repository} after {@link #setup()} that's discarded
 * (deleted) on {@link #teardown()}
 *
 */
@State(Scope.Thread)
public class EmptyRepositoryState {

    private ExternalResources resources;

    private RepoState origin;

    private TemporaryRepository repository;

    /**
     * receive a {@link RepoState} to re-use its {@link RepoState#storageType} parameter, otherwise
     * adding it here and since other required states already depend on {@code RepoState}, we'd be
     * multiplying the number of arguments unnecessarily (i.e. performing the same test twice)
     * 
     * @throws Exception
     */
    public @Setup void setup(ExternalResources resources, RepoState origin) throws Exception {
        this.resources = resources;
        this.origin = origin;
        this.repository = resolveTempRepo();
        this.repository.create();
    }

    public @TearDown void teardown() throws Exception {
        TemporaryRepository repository = this.repository;
        this.repository = null;
        if (repository != null) {
            repository.delete();
        }
    }

    private TemporaryRepository resolveTempRepo() {
        switch (origin.storageType) {
        case "rocksdb":
            return new RocksdbTemporaryRepository(resources);
        case "postgres":
            return new PostgresTemporaryRepository(resources);
        default:
            throw new UnsupportedOperationException(origin.storageType);
        }
    }

    public Repository getRepository() {
        checkNotNull(repository);
        return repository.get();
    }

    private static abstract class TemporaryRepository {

        protected final ExternalResources resources;

        private Repository repository;

        TemporaryRepository(ExternalResources resources) {
            this.resources = resources;
        }

        public Repository get() {
            checkNotNull(repository);
            return this.repository;
        }

        public void create() throws Exception {
            GlobalContextBuilder.builder(new CLIContextBuilder());
            URI repoURI = initialize();
            Hints hints = new Hints().uri(repoURI);
            Context context = GlobalContextBuilder.builder().build(hints);
            this.repository = context.command(InitOp.class).call();
            repository.command(ConfigOp.class).setAction(ConfigAction.CONFIG_SET)
                    .setName("user.name").setValue("benchmark").call();
            repository.command(ConfigOp.class).setAction(ConfigAction.CONFIG_SET)
                    .setName("user.email").setValue("benchmark@geogig.org").call();
        }

        public void delete() throws Exception {
            URI repoURI = repository.getLocation();
            repository.close();
            RepositoryResolver resolver = RepositoryResolver.lookup(repoURI);
            resolver.delete(repoURI);
            deleteInternal(repoURI);
        }

        protected abstract URI initialize() throws Exception;

        protected abstract void deleteInternal(URI repoURI) throws Exception;
    }

    private static class RocksdbTemporaryRepository extends TemporaryRepository {

        RocksdbTemporaryRepository(ExternalResources resources) {
            super(resources);
        }

        protected @Override URI initialize() throws Exception {
            Path tmpdir = resources.getTempdirectory();
            Path clonesdir = tmpdir.resolve("rocks_clones");
            if (!Files.exists(clonesdir)) {
                Files.createDirectory(clonesdir);
            }
            Path repodir = Files.createTempDirectory(clonesdir, "rocksdb_repo");
            return repodir.toUri();
        }

        protected @Override void deleteInternal(URI repoURI) throws Exception {
            // nothing to do, FileRepositoryResolver already deleted the repo directory
        }
    }

    private static class PostgresTemporaryRepository extends TemporaryRepository {

        PostgresTemporaryRepository(ExternalResources resources) {
            super(resources);
        }

        protected @Override URI initialize() throws Exception {
            URI uri = URI.create(
                    "postgresql://localhost/geogig_clone_tests/newrepo?user=postgres&password=geo123");
            Preconditions.checkState(!RepositoryResolver.lookup(uri).repoExists(uri),
                    "repo already exists, delete it manually");
            return uri;
        }

        protected @Override void deleteInternal(URI repoURI) throws Exception {
            // throw new UnsupportedOperationException("not yet implemented");
        }
    }
}
