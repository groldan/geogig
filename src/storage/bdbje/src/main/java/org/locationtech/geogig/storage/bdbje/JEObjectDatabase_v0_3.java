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
import org.locationtech.geogig.storage.datastream.DataStreamSerializationFactoryV3;

import com.google.inject.Inject;

public final class JEObjectDatabase_v0_3 extends JEObjectDatabase {
    @Inject
    public JEObjectDatabase_v0_3(final ConfigDatabase configDB,
            final EnvironmentBuilder envProvider, final Hints hints) {
        this(configDB, envProvider, hints.getBoolean(Hints.OBJECTS_READ_ONLY),
                JEObjectDatabase.ENVIRONMENT_NAME);
    }

    public JEObjectDatabase_v0_3(final ConfigDatabase configDB,
            final EnvironmentBuilder envProvider, final boolean readOnly, final String envName) {
        super(DataStreamSerializationFactoryV3.INSTANCE, configDB, envProvider, readOnly, envName,
                "0.3");
    }
}
