/* Copyright (c) 2013-2016 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.geotools.cli.geopkg;

import java.io.File;

import org.geotools.data.DataStore;
import org.locationtech.geogig.cli.CLICommand;
import org.locationtech.geogig.cli.annotation.ReadOnly;
import org.locationtech.geogig.geotools.cli.base.DataStoreList;
import org.locationtech.geogig.geotools.plumbing.ListOp;

import com.google.common.base.Preconditions;

import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

/**
 * Geopackage CLI proxy for {@link ListOp}
 * 
 * @see ListOp
 */
@ReadOnly
@Command(name = "list", description = "List available feature types in a database")
public class GeopkgList extends DataStoreList implements CLICommand {

    public @ParentCommand GeopkgCommandProxy commonArgs;

    final GeopkgSupport support = new GeopkgSupport();

    protected @Override DataStore getDataStore() {
        File databaseFile = new File(commonArgs.database);
        Preconditions.checkArgument(databaseFile.exists(), "Database file not found.");
        return support.getDataStore(commonArgs);
    }

}
