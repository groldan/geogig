/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.storage.datastream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.RevObject;
import org.locationtech.geogig.storage.RevObjectSerializer;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyOutputStream;

import com.google.common.annotations.Beta;
import com.google.common.io.ByteStreams;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 */
@Beta
public @RequiredArgsConstructor class SnappySerializationFactory implements RevObjectSerializer {

    private final @NonNull RevObjectSerializer factory;

    /**
     * @return {@code false}, this serializer does not support reading back multiple objects from a
     *         single stream where each object has been individually compressed, due to a limitation
     *         in {@code lz-java}
     */
    public @Override boolean supportsStreaming() {
        return false;
    }

    public @Override RevObject read(ObjectId id, InputStream in) throws IOException {
        byte[] uncompressed = Snappy.uncompress(ByteStreams.toByteArray(in));
        return factory.read(id, uncompressed, 0, uncompressed.length);
    }

    public @Override RevObject read(@Nullable ObjectId id, byte[] data, int offset, int length)
            throws IOException {
        byte[] uncompressed = Snappy.uncompress(data);
        return factory.read(id, uncompressed, 0, uncompressed.length);
    }

    public @Override void write(RevObject o, OutputStream out) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        factory.write(o, bout);
        byte[] compressed = Snappy.compress(bout.toByteArray());
        out.write(compressed);
//        SnappyOutputStream cout = new SnappyOutputStream(out);
//        factory.write(o, cout);
//        cout.flush();
    }

    @Override
    public String getDisplayName() {
        return factory.getDisplayName() + "/Snappy";
    }
}
