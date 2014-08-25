/*******************************************************************************
 * Copyright (c) 2014 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *******************************************************************************/
package org.locationtech.geogig.storage.bdbje;

import org.locationtech.geogig.repository.Hints;
import org.locationtech.geogig.storage.ConfigDatabase;

import com.google.inject.Inject;

/**
 * Same as {@link JEGraphDatabase_v0_2} but sets the config version to {@code "0.3"} cause the
 * plugin subsystem doesn't yet support configuring mixed version for the same backend.
 */
public class JEGraphDatabase_v0_3 extends JEGraphDatabase_v0_2 {

    @Inject
    public JEGraphDatabase_v0_3(final ConfigDatabase config, final EnvironmentBuilder envProvider,
            final Hints hints) {
        super(config, envProvider, hints, "0.3");
    }
}
