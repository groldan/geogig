/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.grpc.server;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.locationtech.geogig.grpc.common.Utils.str;
import static org.locationtech.geogig.grpc.common.Utils.tuple;

import java.util.List;
import java.util.Map;

import org.locationtech.geogig.grpc.StringTuple;
import org.locationtech.geogig.grpc.storage.ConfigDatabaseGrpc.ConfigDatabaseImplBase;
import org.locationtech.geogig.grpc.storage.GetQuery;
import org.locationtech.geogig.grpc.storage.PutQuery;
import org.locationtech.geogig.storage.ConfigDatabase;

import com.beust.jcommander.internal.Maps;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.protobuf.StringValue;

import io.grpc.stub.StreamObserver;

public class ConfigDatabaseService extends ConfigDatabaseImplBase {

    private Supplier<ConfigDatabase> _localdb;

    @VisibleForTesting
    public ConfigDatabaseService(ConfigDatabase localdb) {
        this(Suppliers.ofInstance(localdb));
    }

    public ConfigDatabaseService(Supplier<ConfigDatabase> localdb) {
        checkNotNull(localdb);
        this._localdb = localdb;
    }

    private ConfigDatabase localdb() {
        ConfigDatabase database = _localdb.get();
        checkNotNull(database);
        return database;
    }

    @Override
    public void get(GetQuery request, StreamObserver<StringTuple> response) {
        try {
            Map<String, String> result = get(request);
            result.forEach((k, v) -> response.onNext(tuple(k, v)));
            response.onCompleted();
        } catch (RuntimeException e) {
            response.onError(e);
        }
    }

    @Override
    public void getSubsections(GetQuery request, StreamObserver<StringValue> response) {
        String section = request.getSection();
        boolean global = request.getGlobal();

        try {
            List<String> res = global ? localdb().getAllSubsectionsGlobal(section)
                    : localdb().getAllSubsections(section);

            res.forEach((subsection) -> response.onNext(str(subsection)));

            response.onCompleted();
        } catch (RuntimeException e) {
            response.onError(e);
        }
    }

    @Override
    public void put(PutQuery request, StreamObserver<StringTuple> response) {
        String key = request.getKey();
        String value = request.getValue();
        boolean global = request.getGlobal();
        try {
            if (global) {
                localdb().putGlobal(key, value);
            } else {
                localdb().put(key, value);
            }
            response.onNext(tuple(key, value));
            response.onCompleted();
        } catch (RuntimeException e) {
            response.onError(e);
        }
    }

    @Override
    public void remove(GetQuery request, StreamObserver<StringTuple> response) {
        String key = request.getKey();
        String section = request.getSection();
        boolean global = request.getGlobal();
        Map<String, String> removed;
        try {
            if (Strings.isNullOrEmpty(section)) {
                removed = remove(key, global);
            } else {
                removed = removeSection(section, global);
            }
            removed.forEach((k, v) -> response.onNext(tuple(k, v)));
            response.onCompleted();
        } catch (RuntimeException e) {
            response.onError(e);
        }
    }

    private Map<String, String> removeSection(String section, boolean global) {
        Map<String, String> prev = global ? localdb().getAllSectionGlobal(section)
                : localdb().getAllSection(section);
        if (global) {
            localdb().removeSectionGlobal(section);
        } else {
            localdb().removeSection(section);
        }
        return prev;
    }

    private Map<String, String> remove(String key, boolean global) {
        String prev;
        if (global) {
            prev = localdb().getGlobal(key).orNull();
            if (prev != null) {
                localdb().removeGlobal(key);
            }
        } else {
            prev = localdb().get(key).orNull();
            if (prev != null) {
                localdb().remove(key);
            }
        }
        return Maps.newHashMap(key, prev);
    }

    private Map<String, String> get(GetQuery request) {
        boolean all = request.getAll();
        boolean global = request.getGlobal();
        String section = request.getSection();
        String key = request.getKey();

        Map<String, String> res;
        if (Strings.isNullOrEmpty(section)) {
            if (all) {
                res = getAll(global);
            } else {
                res = get(key, global);
            }
        } else {
            res = getAllSection(section, global);
        }
        return res;
    }

    private Map<String, String> getAllSection(String section, boolean global) {
        return global ? localdb().getAllSectionGlobal(section) : localdb().getAllSection(section);
    }

    private Map<String, String> get(String key, boolean global) {
        String val = (global ? localdb().getGlobal(key) : localdb().get(key)).orNull();
        return Maps.newHashMap(key, val);
    }

    private Map<String, String> getAll(boolean global) {
        Map<String, String> val = global ? localdb().getAllGlobal() : localdb().getAll();
        return val;
    }
}
