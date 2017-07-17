/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.grpc.repository;

import static com.google.common.base.Optional.absent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.grpc.StringTuple;
import org.locationtech.geogig.grpc.client.Stubs;
import org.locationtech.geogig.grpc.storage.ConfigDatabaseGrpc.ConfigDatabaseBlockingStub;
import org.locationtech.geogig.grpc.storage.GetQuery;
import org.locationtech.geogig.grpc.storage.GetQuery.Builder;
import org.locationtech.geogig.grpc.storage.PutQuery;
import org.locationtech.geogig.repository.Hints;
import org.locationtech.geogig.storage.ConfigDatabase;
import org.locationtech.geogig.storage.ConfigException;
import org.locationtech.geogig.storage.ConfigException.StatusCode;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.protobuf.StringValue;

import io.grpc.ManagedChannel;

public class ConfigDatabaseClient implements ConfigDatabase {

    private ConfigDatabaseBlockingStub remotedb;

    private final Stubs stubs;

    private static final Map<String, String> OVERRIDES = ImmutableMap.of(//
            "storage.objects", "grpc"//
            , "storage.refs", "grpc"//
            , "storage.graph", "grpc"//
            , "storage.index", "grpc"//
            , "grpc.version", "1"//
    );

    @Inject
    public ConfigDatabaseClient(Hints hints) {
        this.stubs = Stubs.create(hints);
    }

    @VisibleForTesting
    public ConfigDatabaseClient(ManagedChannel channel) {
        this.stubs = Stubs.create(channel);
    }

    private ConfigDatabaseBlockingStub remotedb() {
        if (stubs.open()) {
            remotedb = stubs.newConfigDatabaseBlockingStub();
        }
        return remotedb;
    }

    @Override
    public void close() throws IOException {
        stubs.close();
        this.remotedb = null;
    }

    @Override
    public Optional<String> get(String key) {
        String override = OVERRIDES.get(key);
        if (override != null) {
            return Optional.of(override);
        }
        return get(key, false, String.class);
    }

    @Override
    public Optional<String> getGlobal(String key) {
        return get(key, true, String.class);
    }

    @Override
    public <T> Optional<T> get(String key, Class<T> c) {
        return get(key, false, c);
    }

    @Override
    public <T> Optional<T> getGlobal(String key, Class<T> c) {
        return get(key, true, c);
    }

    @Override
    public Map<String, String> getAll() {
        return get(all(false));
    }

    @Override
    public Map<String, String> getAllGlobal() {
        return get(all(true));
    }

    @Override
    public Map<String, String> getAllSection(String section) {
        return get(section(section, false));
    }

    @Override
    public Map<String, String> getAllSectionGlobal(String section) {
        return get(section(section, true));
    }

    private Map<String, String> get(GetQuery query) {
        Iterator<StringTuple> iterator = remotedb().get(query);
        Map<String, String> res = new HashMap<>();
        iterator.forEachRemaining((t) -> res.put(t.getKey(), t.getValue()));
        return res;
    }

    @Override
    public List<String> getAllSubsections(String section) {
        return subsections(subsection(section, false));
    }

    @Override
    public List<String> getAllSubsectionsGlobal(String section) {
        return subsections(subsection(section, true));
    }

    private List<String> subsections(GetQuery section) {
        Iterator<StringValue> iterator = remotedb().getSubsections(section);
        List<String> res = new ArrayList<>();
        iterator.forEachRemaining((s) -> res.add(s.getValue()));
        return res;
    }

    @Override
    public void put(String key, Object value) {
        put(key, value, false);
    }

    @Override
    public void putGlobal(String key, Object value) {
        put(key, value, true);
    }

    private void put(String key, Object value, boolean global) {

        PutQuery request = PutQuery.newBuilder().setKey(key)
                .setValue(value == null ? "" : value.toString()).setGlobal(global).build();

        remotedb().put(request);
    }

    @Override
    public void remove(String key) {
        remove(key, null, false);
    }

    @Override
    public void removeGlobal(String key) {
        remove(key, null, true);
    }

    @Override
    public void removeSection(String section) {
        remove(null, section, false);
    }

    @Override
    public void removeSectionGlobal(String section) {
        remove(null, section, true);
    }

    private void remove(String key, String section, boolean global) {
        GetQuery request;
        if (key == null) {
            request = section(section, global);
        } else {
            request = key(key, global);
        }
        remotedb().remove(request);
    }

    private GetQuery all(boolean global) {
        return query(null, global, true, false, null);
    }

    private GetQuery section(String section, boolean global) {
        return query(null, global, true, false, section);
    }

    private GetQuery subsection(String section, boolean global) {
        return query(null, global, true, true, section);
    }

    private GetQuery key(String key, boolean global) {
        return query(key, global, false, false, null);
    }

    private GetQuery query(@Nullable String key, boolean global, boolean all, boolean subsections,
            @Nullable String section) {
        Builder builder = GetQuery.newBuilder();
        if (key != null)
            builder.setKey(key);
        builder.setGlobal(global);
        builder.setAll(all);
        if (section != null)
            builder.setSection(section);
        builder.setSubsections(subsections);
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    private <T> T cast(Class<T> c, String s) {
        if (String.class.equals(c)) {
            return c.cast(s);
        }
        if (int.class.equals(c) || Integer.class.equals(c)) {
            return (T) Integer.valueOf(s);
        }
        if (Boolean.class.equals(c)) {
            return c.cast(Boolean.valueOf(s));
        }
        throw new IllegalArgumentException("Unsupported type: " + c);
    }

    private <T> Optional<T> get(String key, boolean global, Class<T> castTo) {
        if (key == null) {
            throw new ConfigException(StatusCode.SECTION_OR_NAME_NOT_PROVIDED);
        }
        final int keySeparatorIndex = key.lastIndexOf('.');
        if (-1 == keySeparatorIndex) {
            throw new ConfigException(ConfigException.StatusCode.SECTION_OR_NAME_NOT_PROVIDED);
        }
        if (0 == keySeparatorIndex) {
            throw new ConfigException(ConfigException.StatusCode.MISSING_SECTION);
        }
        if (key.length() == 1 + keySeparatorIndex) {
            throw new ConfigException(ConfigException.StatusCode.SECTION_OR_NAME_NOT_PROVIDED);
        }

        Iterator<StringTuple> iterator = remotedb().get(key(key, global));
        String value = null;
        if (iterator.hasNext()) {
            value = iterator.next().getValue();
        }
        if (Strings.isNullOrEmpty(value)) {
            return absent();
        }
        T casted = cast(castTo, value);
        return Optional.of(casted);
    }
}
