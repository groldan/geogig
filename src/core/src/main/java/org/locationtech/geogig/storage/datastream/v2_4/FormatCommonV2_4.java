/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * David Blasby (Boundless) - initial implementation
 */
package org.locationtech.geogig.storage.datastream.v2_4;

import static com.google.common.base.Preconditions.checkState;
import static org.locationtech.geogig.storage.datastream.Varint.readUnsignedVarInt;
import static org.locationtech.geogig.storage.datastream.Varint.readUnsignedVarLong;
import static org.locationtech.geogig.storage.datastream.Varint.writeUnsignedVarInt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.model.Bucket;
import org.locationtech.geogig.model.Node;
import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.RevObject;
import org.locationtech.geogig.model.RevObject.TYPE;
import org.locationtech.geogig.model.RevTree;
import org.locationtech.geogig.model.impl.RevTreeImplDelta;
import org.locationtech.geogig.model.impl.RevTreeImplDelta.DeltaBucket;
import org.locationtech.geogig.model.impl.RevTreeImplDelta.DeltaNode;
import org.locationtech.geogig.storage.ObjectStore;
import org.locationtech.geogig.storage.datastream.FormatCommonV2_2;
import org.locationtech.geogig.storage.datastream.Varint;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.vividsolutions.jts.geom.Envelope;

public class FormatCommonV2_4 extends FormatCommonV2_2 {

    private ObjectStore source;

    public FormatCommonV2_4(ObjectStore source) {
        Preconditions.checkNotNull(source);
        this.source = source;
    }

    @Override
    public void writeTree(RevTree tree, DataOutput data) throws IOException {
        if (tree instanceof RevTreeImplDelta) {
            RevTreeImplDelta d = (RevTreeImplDelta) tree;
            final int deltaLevel = d.getDeltaLevel();
            data.writeByte(deltaLevel);
            final ObjectId originalTreeId = ((RevTreeImplDelta) tree).getOriginalId();
            originalTreeId.writeTo(data);
        } else {
            data.writeByte(0);
        }
        super.writeTree(tree, data);
    }

    @Override
    public RevTree readTree(@Nullable ObjectId id, DataInput in) throws IOException {
        final int deltaLevel = in.readByte() & 0xFF;
        if (deltaLevel == 0) {
            return super.readTree(id, in);
        }
        final ObjectId originalTreeId = readObjectId(in);

        final long size = readUnsignedVarLong(in);
        final int treeCount = readUnsignedVarInt(in);

        final ImmutableList.Builder<Node> featuresBuilder = new ImmutableList.Builder<Node>();
        final ImmutableList.Builder<Node> treesBuilder = new ImmutableList.Builder<Node>();

        final int nFeatures = readUnsignedVarInt(in);
        for (int i = 0; i < nFeatures; i++) {
            Node n = readNode(in);
            checkState(RevObject.TYPE.FEATURE.equals(n.getType()),
                    "Non-feature node in tree's feature list.");
            featuresBuilder.add(n);
        }

        final int nTrees = readUnsignedVarInt(in);
        for (int i = 0; i < nTrees; i++) {
            Node n = readNode(in);
            checkState(RevObject.TYPE.TREE.equals(n.getType()),
                    "Non-tree node in tree's subtree list %s->%s.", n.getType(), n);

            treesBuilder.add(n);
        }

        final int nBuckets = readUnsignedVarInt(in);
        final SortedMap<Integer, Bucket> buckets;
        buckets = nBuckets > 0 ? new TreeMap<>() : ImmutableSortedMap.of();
        for (int i = 0; i < nBuckets; i++) {
            int bucketIndex = readUnsignedVarInt(in);
            {
                Integer idx = Integer.valueOf(bucketIndex);
                checkState(!buckets.containsKey(idx), "duplicate bucket index: %s", idx);
                // checkState(bucketIndex < RevTree.MAX_BUCKETS, "Illegal bucket index: %s", idx);
            }
            Bucket bucket = readBucketBody(in);
            buckets.put(Integer.valueOf(bucketIndex), bucket);
        }
        checkState(nBuckets == buckets.size(), "expected %s buckets, got %s", nBuckets,
                buckets.size());
        ImmutableList<Node> trees = treesBuilder.build();
        ImmutableList<Node> features = featuresBuilder.build();

        ImmutableSortedMap<Integer, Bucket> bkts = ImmutableSortedMap.copyOf(buckets);

        RevTree tree = new RevTreeImplDelta(source, originalTreeId, deltaLevel, id, size, treeCount,
                trees, features, bkts);
        return tree;
    }

    @Override
    public void writeBucket(final int index, final Bucket bucket, DataOutput data, Envelope envBuff)
            throws IOException {
        if (bucket instanceof RevTreeImplDelta.DeltaBucket) {
            writeUnsignedVarInt(index, data);
            data.writeByte(-1);

            RevTreeImplDelta.DeltaBucket db = (DeltaBucket) bucket;
            Integer origIndex = db.index;
            Varint.writeUnsignedVarInt(origIndex.intValue(), data);
        } else {
            writeUnsignedVarInt(index, data);
            data.writeByte(1);

            data.write(bucket.getObjectId().getRawValue());
            envBuff.setToNull();
            bucket.expand(envBuff);
            writeBounds(envBuff, data);
        }
    }

    @Override
    protected final Bucket readBucketBody(DataInput in) throws IOException {
        int mode = in.readByte();
        if (mode == -1) {
            int index = Varint.readUnsignedVarInt(in);
            return new RevTreeImplDelta.DeltaBucket(Integer.valueOf(index));
        }
        Preconditions.checkState(mode == 1, "expected mode = 1, got %s", mode);
        return super.readBucketBody(in);
    }

    @Override
    public Node readNode(DataInput in) throws IOException {
        int mode = in.readByte();
        if (mode == -1) {
            TYPE type = TYPE.valueOf(in.readByte() & 0xFF);
            int index = Varint.readUnsignedVarInt(in);
            return new RevTreeImplDelta.DeltaNode(type, index);
        }
        Preconditions.checkState(mode == 1, "expected mode = 1, got %s", mode);
        return super.readNode(in);
    }

    @Override
    public void writeNode(Node node, DataOutput data, Envelope env) throws IOException {
        if (node instanceof RevTreeImplDelta.DeltaNode) {
            data.write(-1);
            RevTreeImplDelta.DeltaNode dn = (DeltaNode) node;
            data.writeByte(dn.type.value());
            Varint.writeUnsignedVarInt(dn.index, data);
        } else {
            data.write(1);
            super.writeNode(node, data, env);
        }
    }

}
