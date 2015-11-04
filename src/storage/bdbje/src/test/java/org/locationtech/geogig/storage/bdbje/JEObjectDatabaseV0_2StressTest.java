package org.locationtech.geogig.storage.bdbje;

import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.repository.Hints;
import org.locationtech.geogig.storage.ConfigDatabase;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.locationtech.geogig.storage.ObjectDatabaseStressTest;

public class JEObjectDatabaseV0_2StressTest extends ObjectDatabaseStressTest {

    @Override
    protected ObjectDatabase createDb(Platform platform, ConfigDatabase config) {
        Hints hints = new Hints();
        EnvironmentBuilder envProvider = new EnvironmentBuilder(platform, hints);
        JEObjectDatabase_v0_2 objectdb = new JEObjectDatabase_v0_2(config, envProvider, hints);
        return objectdb;
    }

}
