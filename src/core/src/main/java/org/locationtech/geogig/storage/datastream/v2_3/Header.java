/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.storage.datastream.v2_3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.RevObject;
import org.locationtech.geogig.model.RevObject.TYPE;
import org.locationtech.geogig.model.RevTree;
import org.locationtech.geogig.storage.datastream.Delta;
import org.locationtech.geogig.storage.datastream.Varint;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * {@code
 *  HEADER =    <byte>, (* literal byte 1, corresponding to RevObject.TYPE.TREE *) 
 *              <ushort>, (* size of header *)
 *              <uvarlong>, (*total size*)
 *              <uvarint>, (* recursive number of tree nodes *)
 *              <uvarint>, (*offset of contained tree nodes nodeset*)
 *              <uvarint>, (*offset of contained feature nodes nodeset*)
 *              <uvarint>, (*offset of contained bucketset*)
 *              <uvarint>; (*byte offset of string table, zero being the first byte of the header*)
 * }
 *
 */
class Header {

    public static final Header EMPTY = new Header(0, 0, 0, null);

    private final long size;

    private final int trees;

    private final int deltaLevel;

    private @Nullable ObjectId originalTreeId;

    public Header(long size, int trees, int deltaLevel, @Nullable ObjectId originalTreeId) {
        this.size = size;
        this.trees = trees;
        this.deltaLevel = deltaLevel;
        this.originalTreeId = originalTreeId;
    }

    public long size() {
        return size;
    }

    public int numTrees() {
        return trees;
    }

    public int deltaLevel() {
        return deltaLevel;
    }

    public ObjectId originalId() {
        return deltaLevel == 0 ? RevTree.EMPTY_TREE_ID : originalTreeId;
    }

    public static void encode(DataOutput out, RevTree tree) throws IOException {
        out.write(RevObject.TYPE.TREE.ordinal());
        final long totalSize = tree.size();
        final int totalSubtrees = tree.numTrees();
        final int deltaLevel = tree instanceof Delta ? ((Delta) tree).getDeltaLevel() : 0;
        Varint.writeUnsignedVarLong(totalSize, out);
        Varint.writeUnsignedVarInt(totalSubtrees, out);
        Varint.writeUnsignedVarInt(deltaLevel, out);
        if (deltaLevel > 0) {
            ((Delta) tree).getOriginalId().writeTo(out);
        }
    }

    public static Header parse(ByteBuffer data) {
        DataInput in = RevTreeFormat.asDataInput(data);
        try {
            final int type = in.readUnsignedByte();
            TYPE _type = TYPE.valueOf(type);
            Preconditions.checkArgument(TYPE.TREE.equals(_type));
            final long size = Varint.readUnsignedVarLong(in);
            final int trees = Varint.readUnsignedVarInt(in);
            final int deltaLevel = Varint.readUnsignedVarInt(in);
            ObjectId originalId = null;
            if (deltaLevel > 0) {
                byte[] oidbuff = new byte[ObjectId.NUM_BYTES];
                in.readFully(oidbuff);
                originalId = ObjectId.createNoClone(oidbuff);
            }
            return new Header(size, trees, deltaLevel, originalId);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

}
