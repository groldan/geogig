/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.grpc.server;

import static org.locationtech.geogig.grpc.common.Constants.DEFAULT_PORT;

import java.io.IOException;
import java.util.List;

import org.locationtech.geogig.repository.Repository;
import org.locationtech.geogig.repository.RepositoryConnectionException;
import org.locationtech.geogig.repository.impl.MultiRepositoryResolver;
import org.locationtech.geogig.storage.ConfigDatabase;
import org.locationtech.geogig.storage.GraphDatabase;
import org.locationtech.geogig.storage.IndexDatabase;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.locationtech.geogig.storage.RefDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.grpc.BindableService;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

public class Server {
    private static final Logger logger = LoggerFactory.getLogger(Server.class.getName());

    private static final ThreadLocal<Repository> REPO = new ThreadLocal<>();

    /* The port on which the server should run */
    private int port = DEFAULT_PORT;

    private io.grpc.Server server;

    private MultiRepositoryResolver repos;

    public Server(MultiRepositoryResolver repos) {
        this.repos = repos;
    }

    public Server port(int port) {
        this.port = port;
        return this;
    }

    public Server start() throws IOException {

        server = ServerBuilder.forPort(port)//
                .addService(repoSetterInterceptor(new RefDatabaseService(() -> refs())))//
                .addService(repoSetterInterceptor(new ConfigDatabaseService(() -> config())))//
                .addService(repoSetterInterceptor(new GraphDatabaseService(() -> grpah())))//
                .addService(repoSetterInterceptor(new IndexDatabaseService(() -> index())))//
                .addService(repoSetterInterceptor(new ObjectDatabaseService(() -> objects())))//
                .addService(repoSetterInterceptor(new FeatureService(() -> repo())))//
                .build().start();

        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                Server.this.stop();
                System.err.println("*** server shut down");
            }
        });

        List<String> repositories = repos.findRepositories();
        System.err.printf("%,d repos at root URI: %s", repositories.size(), repositories);
        return this;
    }

    private Repository repo() {
        Repository repo = REPO.get();
        if (null == repo) {
            throw new NullPointerException("Repository not set");
        }
        return repo;
    }

    private ObjectDatabase objects() {
        return repo().objectDatabase();
    }

    private IndexDatabase index() {
        return repo().indexDatabase();
    }

    private GraphDatabase grpah() {
        return repo().graphDatabase();
    }

    private ConfigDatabase config() {
        return repo().configDatabase();
    }

    private RefDatabase refs() {
        return repo().refDatabase();
    }

    private ServerServiceDefinition repoSetterInterceptor(BindableService service) {
        ServerInterceptor interceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                    Metadata headers, ServerCallHandler<ReqT, RespT> next) {

                Key<String> key = Key.of("repo", Metadata.ASCII_STRING_MARSHALLER);
                String repository = headers.get(key);
                Repository repo = null;
                if (null == repository) {
                    System.err.println("Got repository: " + repository);
                } else {
                    try {
                        repo = repos.getRepository(repository);
                    } catch (RepositoryConnectionException e) {
                        throw new StatusRuntimeException(Status.NOT_FOUND);
                    }
                    Preconditions.checkNotNull(repo);
                    REPO.set(repo);
                }
                Listener<ReqT> listener = next.startCall(call, headers);
                Listener<ReqT> cleaner = new RepoThreadLocalCleaner<ReqT>(repo, listener);
                return cleaner;
            }
        };
        ServerServiceDefinition definition = ServerInterceptors.intercept(service, interceptor);
        return definition;
    }

    private static class RepoThreadLocalCleaner<ReqT>
            extends ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT> {

        private Repository repository;

        protected RepoThreadLocalCleaner(Repository repo, Listener<ReqT> delegate) {
            super(delegate);
            repository = repo;
        }

        @Override
        public void onReady() {
            REPO.set(repository);
        }

        @Override
        public void onCancel() {
            REPO.remove();
        }

        @Override
        public void onComplete() {
            REPO.remove();
        }
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static Server forRepos(MultiRepositoryResolver resolver) {
        return new Server(resolver);
    }
    //
    // public static Server forRepository(Repository repository) {
    // final Server server = new Server(repository);
    // return server;
    // }
}
