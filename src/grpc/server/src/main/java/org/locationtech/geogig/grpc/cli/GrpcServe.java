/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.grpc.cli;

import static org.locationtech.geogig.grpc.common.Constants.DEFAULT_PORT;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.locationtech.geogig.cli.AbstractCommand;
import org.locationtech.geogig.cli.CommandFailedException;
import org.locationtech.geogig.cli.Console;
import org.locationtech.geogig.cli.GeogigCLI;
import org.locationtech.geogig.cli.InvalidParameterException;
import org.locationtech.geogig.cli.annotation.RequiresRepository;
import org.locationtech.geogig.grpc.server.Server;
import org.locationtech.geogig.repository.Repository;
import org.locationtech.geogig.repository.RepositoryResolver;
import org.locationtech.geogig.repository.impl.MultiRepositoryResolver;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

/**
 * This command starts GRPC server to serve up a repository.
 * <p>
 * Usage:
 * <ul>
 * <li>{@code geogig grpc [-p <port>] [<repository URI>]}
 * </ul>
 * </p>
 * 
 * @see Server
 */
@RequiresRepository(false)
@Parameters(commandNames = "grpc", commandDescription = "Serves a repository through the GRPC protocol")
public class GrpcServe extends AbstractCommand {

    @Parameter(description = "Repository location (URI).", required = false, arity = 1)
    private List<String> repo;

    @Parameter(names = { "--multirepo",
            "-m" }, description = "Serve all of the repositories in the directory.", required = false)
    private boolean multiRepo = false;

    @Parameter(names = { "--port", "-p" }, description = "Port to run server on")
    private int port = DEFAULT_PORT;

    @Override
    protected void runInternal(GeogigCLI cli)
            throws InvalidParameterException, CommandFailedException, IOException {

        final Console console = cli.getConsole();

        final String loc = repo != null && repo.size() > 0 ? repo.get(0) : null;

        final MultiRepositoryResolver provider;

        URI repoURI = null;
        if (cli.getGeogig() != null && cli.getGeogig().isOpen()) {
            if (loc != null) {
                throw new CommandFailedException(
                        "Cannot specify a repository from within a repository.");
            }
            Repository repository = cli.getGeogig().getRepository();
            repoURI = repository.getLocation();
            provider = MultiRepositoryResolver.of(repository);
        } else {
            try {
                repoURI = RepositoryResolver.resolveRepoUriFromString(cli.getPlatform(),
                        loc == null ? "." : loc);
            } catch (URISyntaxException e) {
                throw new CommandFailedException("Unable to parse the root repository URI.", e);
            }

            if (multiRepo) {
                provider = new MultiRepositoryResolver(repoURI);
            } else {
                provider = new MultiRepositoryResolver(repoURI);
            }
        }

        console.println(String.format("Starting RPC server on port %d, use CTRL+C to exit.", port));

        try {
            Server server = Server.forRepos(provider).port(port).start();
            console.println("Server started.");
            server.blockUntilShutdown();
        } catch (Exception e) {
            throw new CommandFailedException("Unable to start server", e);
        }
    }
}
