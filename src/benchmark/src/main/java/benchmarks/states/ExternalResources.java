/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package benchmarks.states;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.System.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import com.beust.jcommander.internal.Nullable;
import com.google.common.base.Strings;

@State(Scope.Benchmark)
public class ExternalResources {

    static final String DEFAULT_ROCKS_URI_TEMPLATE = "/data2/repositories/GIG-880_clone_throughput/${repo}";

    static final String DEFAULT_PG_URI_TEMPLATE = "postgresql://localhost/geogig_release_test_data/${repo}?user=geogig&password=geo123";

    static final String SYSTEM_TMPDIR = System.getProperty("java.io.tmpdir");

    static final String ENV_TMPDIR = "TMPDIR";

    static final String ENV_ROCKS_TEMPLATE = "ROCKS_TEMPLATE";

    static final String ENV_PG_TEMPLATE = "PG_TEMPLATE";

    static final String SERIALIZED_STATES_FOLDER_NAME = "benchmark-serialized-states";

    private Path tmpDir;

    private Path serializedStatesFolder;

    private String rocksURITemplate, pgURITemplate;

    public @Setup void setup() throws Exception {
        this.tmpDir = Paths.get(resolveEnv(ENV_TMPDIR, SYSTEM_TMPDIR)).toAbsolutePath();
        checkState(Files.exists(tmpDir) && Files.isDirectory(tmpDir) && Files.isWritable(tmpDir),
                "Make sure %s exists and is a writable directory", tmpDir);

        this.rocksURITemplate = resolveTemplateURI(ENV_ROCKS_TEMPLATE, DEFAULT_ROCKS_URI_TEMPLATE);
        this.pgURITemplate = resolveTemplateURI(ENV_PG_TEMPLATE, DEFAULT_PG_URI_TEMPLATE);
        this.serializedStatesFolder = resolveSerializedStatesFolder();
    }

    private Path resolveSerializedStatesFolder() throws IOException {
        Path path = tmpDir.resolve(SERIALIZED_STATES_FOLDER_NAME);
        if (Files.exists(path)) {
            checkState(Files.isDirectory(path) && Files.isWritable(path),
                    "Make sure %s exists and is a writable directory", path);
        } else {
            Files.createDirectory(path);
        }
        return path;
    }

    private String resolveTemplateURI(String envVar, String defaultValue) {
        String val = resolveEnv(envVar, defaultValue);
        checkState(!Strings.isNullOrEmpty(val), "%s not provided");
        checkState(val.contains("${repo}"),
                "Environment variable %s must contain ${repo} as repo name placeholder: ", val);
        return val;
    }

    private String resolveEnv(String varname, @Nullable String defaultValue) {
        String value = getenv(varname);
        if (Strings.isNullOrEmpty(value)) {
            value = defaultValue;
        }
        return value;
    }

    public Path getTempdirectory() {
        return tmpDir;
    }

    public Path getSerializedStatesFolder() {
        return serializedStatesFolder;
    }

    public String getRocksURITemplate() {
        return rocksURITemplate;
    }

    public String getPostgresURITemplate() {
        return pgURITemplate;
    }
}
