package org.locationtech.geogig.test.integration.sqlite;

import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.storage.ConfigDatabase;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.locationtech.geogig.storage.ObjectDatabaseStressTest;
import org.locationtech.geogig.storage.sqlite.XerialObjectDatabaseV2;

public class XerialObjectDatabaseV2StressTest extends ObjectDatabaseStressTest {

    @Override
    protected ObjectDatabase createDb(Platform platform, ConfigDatabase config) {
        
        XerialObjectDatabaseV2 objectdb = new XerialObjectDatabaseV2(config, platform, null);
        return objectdb;
    }

}
