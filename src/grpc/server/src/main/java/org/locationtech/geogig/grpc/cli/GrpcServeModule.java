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

import org.locationtech.geogig.cli.CLIModule;

import com.google.inject.Binder;

/**
 * Hooks up into the CLI commands through the {@link CLIModule} SPI lookup by means of the
 * {@code META-INF/services/org.locationtech.geogig.cli.CLIModule} text file, and binds the
 * {@link GrpcServe} command.
 *
 */
public class GrpcServeModule implements CLIModule {

    @Override
    public void configure(Binder binder) {
        binder.bind(GrpcServe.class);
    }

}
