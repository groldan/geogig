package org.locationtech.geogig.storage.sqlite;

import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.storage.ConfigDatabase;

import com.google.inject.Inject;

public class XerialGraphDatabaseV2 extends XerialGraphDatabase {

    @Inject
    public XerialGraphDatabaseV2(ConfigDatabase configdb, Platform platform) {
        super(configdb, platform, XerialStorageProviderV2.GRAPH);
    }

}
