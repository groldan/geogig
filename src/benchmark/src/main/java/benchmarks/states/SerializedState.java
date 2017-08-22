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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.RevObject;
import org.locationtech.geogig.storage.datastream.v2_3.DataStreamSerializationFactoryV2_3;
import org.locationtech.geogig.storage.impl.ObjectSerializingFactory;
import org.locationtech.geogig.storage.impl.PersistedIterable;
import org.locationtech.geogig.storage.impl.PersistedIterable.Serializer;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.hash.HashCode;

@State(Scope.Benchmark)
public class SerializedState {

    private Path repoSerialDataFolder;

    public static Serializer<ObjectId> OBJECTID = new Serializer<ObjectId>() {

        @Override
        public void write(DataOutputStream out, ObjectId value) throws IOException {
            value.writeTo(out);
        }

        @Override
        public ObjectId read(DataInputStream in) throws IOException {
            return ObjectId.readFrom(in);
        }
    };

    public static Serializer<? extends RevObject> REVOBJECT = new Serializer<RevObject>() {

        private final ObjectSerializingFactory fac = DataStreamSerializationFactoryV2_3.INSTANCE;

        @Override
        public void write(DataOutputStream out, RevObject value) throws IOException {
            OBJECTID.write(out, value.getId());
            fac.write(value, out);
        }

        @Override
        public RevObject read(DataInputStream in) throws IOException {
            ObjectId id = OBJECTID.read(in);
            RevObject object = fac.read(id, in);
            return object;
        }
    };

    public @Setup void initialize(ExternalResources resources, RepoState repo) throws IOException {
        final Path serializedStatesFolder = resources.getSerializedStatesFolder();
        final URI repoURI = repo.getRepoURI();
        final String dirName;
        {
            HashCode hashcode = ObjectId.HASH_FUNCTION.hashString(repoURI.toString(),
                    Charsets.UTF_8);
            String hexhash = hashcode.toString();
            dirName = hexhash.substring(0, 8);
        }

        Path repoPath = serializedStatesFolder.resolve(dirName);
        if (Files.exists(repoPath)) {
            Preconditions.checkState(Files.isDirectory(repoPath));
        } else {
            Files.createDirectory(repoPath);
            System.err.printf("## Created temporary directory %s for repo %s\n", repoPath,
                    repo.getRepoURI());
            Path uriFile = repoPath.resolve("repo.url");
            Files.write(uriFile, (repoURI.toString() + "\n").getBytes(Charsets.UTF_8));
        }
        this.repoSerialDataFolder = repoPath;
    }

    public boolean exists(String streamName) {
        return Files.exists(pathFor(streamName));
    }

    public <T> PersistedIterable<T> get(String streamName,
            PersistedIterable.Serializer<T> serializer) {

        Path file = pathFor(streamName);
        PersistedIterable<T> iterable = PersistedIterable.create(file, serializer, false, true,
                false);
        return iterable;
    }

    private Path pathFor(String streamName) {
        String fileName = streamName + ".ser";
        Path file = repoSerialDataFolder.resolve(fileName);
        return file;
    }
}
