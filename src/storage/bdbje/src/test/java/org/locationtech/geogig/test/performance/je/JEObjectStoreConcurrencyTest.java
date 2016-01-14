/* Copyright (c) 2016 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Erik Merkle (Boundless) - initial implementation
 */
package org.locationtech.geogig.test.performance.je;

import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.repository.Hints;
import org.locationtech.geogig.storage.ConfigDatabase;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.locationtech.geogig.storage.bdbje.EnvironmentBuilder;
import org.locationtech.geogig.storage.bdbje.JEObjectDatabase_v0_1;
import org.locationtech.geogig.storage.fs.IniFileConfigDatabase;
import org.locationtech.geogig.test.performance.ObjectStoreConcurrencyTest;

/**
 *
 */
public class JEObjectStoreConcurrencyTest extends ObjectStoreConcurrencyTest {
    @Override
    protected ObjectDatabase createOpen(Platform platform, Hints hints) {
        EnvironmentBuilder envProvider;
        envProvider = new EnvironmentBuilder(platform, null);
        ConfigDatabase configDB = new IniFileConfigDatabase(platform);
        ObjectDatabase db = new JEObjectDatabase_v0_1(configDB, envProvider, hints);
        db.open();
        return db;
    }

}
