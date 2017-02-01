package org.locationtech.geogig.di;

import org.locationtech.geogig.repository.AbstractGeoGigOp;
import org.locationtech.geogig.repository.Context;
import org.locationtech.geogig.repository.Platform;
import org.locationtech.geogig.repository.Repository;
import org.locationtech.geogig.repository.StagingArea;
import org.locationtech.geogig.repository.WorkingTree;
import org.locationtech.geogig.storage.BlobStore;
import org.locationtech.geogig.storage.ConfigDatabase;
import org.locationtech.geogig.storage.ConflictsDatabase;
import org.locationtech.geogig.storage.GraphDatabase;
import org.locationtech.geogig.storage.IndexDatabase;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.locationtech.geogig.storage.PluginDefaults;
import org.locationtech.geogig.storage.RefDatabase;

public final class ReadOnlyContext implements Context {

    private final Context context;

    private RefDatabase readOnlyRefs;

    public ReadOnlyContext(Context context) {
        this.context = context;
    }

    @Override
    public <T extends AbstractGeoGigOp<?>> T command(Class<T> commandClass) {
        T command = context.command(commandClass);
        command.setContext(this);
        return command;
    }

    @Override
    public RefDatabase refDatabase() {
        if (readOnlyRefs == null) {
            RefDatabase refDatabase = context.refDatabase();
            readOnlyRefs = new RefDatabaseSnapshot(refDatabase);
        }
        return readOnlyRefs;
        // return context.refDatabase();
    }

    @Override
    public WorkingTree workingTree() {
        return context.workingTree();
    }

    @Override
    public StagingArea index() {
        return context.index();
    }

    @Override
    public Platform platform() {
        return context.platform();
    }

    @Override
    public ObjectDatabase objectDatabase() {
        return context.objectDatabase();
    }

    @Override
    public ConflictsDatabase conflictsDatabase() {
        return context.conflictsDatabase();
    }

    @Override
    public ConfigDatabase configDatabase() {
        return context.configDatabase();
    }

    @Override
    public GraphDatabase graphDatabase() {
        return context.graphDatabase();
    }

    @Override
    public Repository repository() {
        return context.repository();
    }

    @Override
    public BlobStore blobStore() {
        return context.blobStore();
    }

    @Override
    public PluginDefaults pluginDefaults() {
        return context.pluginDefaults();
    }

    @Override
    public StagingArea stagingArea() {
        return context.stagingArea();
    }

    @Override
    public IndexDatabase indexDatabase() {
        return context.indexDatabase();
    }

}