package org.locationtech.geogig.test.integration.sqlite;

import org.junit.Ignore;
import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.storage.ConfigDatabase;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.locationtech.geogig.storage.ObjectDatabaseStressTest;
import org.locationtech.geogig.storage.sqlite.XerialObjectDatabaseV1;

@Ignore
public class XerialObjectDatabaseV1StressTest extends ObjectDatabaseStressTest {

    @Override
    protected ObjectDatabase createDb(Platform platform, ConfigDatabase config) {

        XerialObjectDatabaseV1 objectdb = new XerialObjectDatabaseV1(config, platform, null);
        return objectdb;
    }

}
