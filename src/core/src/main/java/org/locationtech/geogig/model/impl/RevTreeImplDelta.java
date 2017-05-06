package org.locationtech.geogig.model.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.model.Bucket;
import org.locationtech.geogig.model.Node;
import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.RevTree;
import org.locationtech.geogig.plumbing.HashObject;
import org.locationtech.geogig.storage.ObjectStore;
import org.locationtech.geogig.storage.datastream.Delta;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import com.vividsolutions.jts.geom.Envelope;

public class RevTreeImplDelta extends AbstractRevObject implements RevTree, Delta {

    private final Supplier<RevTree> original;

    private ImmutableList<Node> trees;

    private ImmutableList<Node> features;

    private ImmutableSortedMap<Integer, Bucket> buckets;

    private final long size;

    private final int numTrees;

    private final ObjectId originalTreeId;

    private final int deltaLevel;

    public RevTreeImplDelta(ObjectStore source, //
            ObjectId originalTreeId, //
            int deltaLevel, //
            ObjectId id, //
            long size, //
            int numtrees, //
            ImmutableList<Node> trees, //
            ImmutableList<Node> features, //
            ImmutableSortedMap<Integer, Bucket> buckets) {
        super(id);
        this.originalTreeId = originalTreeId;
        this.deltaLevel = deltaLevel;
        this.original = Suppliers.memoize(() -> source.getTree(this.originalTreeId));
        this.size = size;
        this.numTrees = numtrees;
        this.trees = trees;
        this.features = features;
        this.buckets = buckets;

        features.forEach((n) -> {
            if (n instanceof DeltaNode) {
                ((DeltaNode) n).setTree(RevTreeImplDelta.this);
            }
        });
        trees.forEach((n) -> {
            if (n instanceof DeltaNode) {
                ((DeltaNode) n).setTree(RevTreeImplDelta.this);
            }
        });

        buckets.values().forEach((b) -> {
            if (b instanceof DeltaBucket) {
                ((DeltaBucket) b).setTree(RevTreeImplDelta.this);
            }
        });
    }

    public RevTreeImplDelta(//
            RevTree original, //
            int deltaLevel, ObjectId id, //
            long size, //
            int numtrees, //
            @Nullable ImmutableList<Node> trees, //
            @Nullable ImmutableList<Node> features, //
            @Nullable ImmutableSortedMap<Integer, Bucket> buckets) {

        super(id);
        this.originalTreeId = original.getId();
        this.deltaLevel = deltaLevel;
        this.original = () -> original;
        this.size = size;
        this.numTrees = numtrees;

        ImmutableList<Node> deltaTrees = delta(original.trees(), trees);
        ImmutableList<Node> deltaFeatures = delta(original.features(), features);
        ImmutableSortedMap<Integer, Bucket> deltaBuckets = delta(original.buckets(), buckets);

        ObjectId id2 = HashObject.hashTree(deltaTrees, deltaFeatures, deltaBuckets);
        Preconditions.checkState(id.equals(id2));
        this.trees = deltaTrees;
        this.features = deltaFeatures;
        this.buckets = deltaBuckets;

        deltaFeatures.forEach((n) -> {
            if (n instanceof DeltaNode) {
                ((DeltaNode) n).setTree(RevTreeImplDelta.this);
            }
        });
        deltaTrees.forEach((n) -> {
            if (n instanceof DeltaNode) {
                ((DeltaNode) n).setTree(RevTreeImplDelta.this);
            }
        });

        deltaBuckets.values().forEach((b) -> {
            if (b instanceof DeltaBucket) {
                ((DeltaBucket) b).setTree(RevTreeImplDelta.this);
            }
        });
    }

    @Override
    public int getDeltaLevel() {
        return deltaLevel;
    }

    @Override
    public ObjectId getOriginalId() {
        return originalTreeId;
    }

    private RevTree original() {
        return original.get();
    }

    @Override
    public TYPE getType() {
        return TYPE.TREE;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public int numTrees() {
        return numTrees;
    }

    @Override
    public ImmutableList<Node> trees() {
        return trees;
    }

    @Override
    public ImmutableList<Node> features() {
        return features;
    }

    @Override
    public ImmutableSortedMap<Integer, Bucket> buckets() {
        return buckets;
    }

    public static RevTree build(final RevTree original, final long size, final int childTreeCount,
            @Nullable ImmutableList<Node> trees, @Nullable ImmutableList<Node> features,
            @Nullable ImmutableSortedMap<Integer, Bucket> buckets) {

        ObjectId id = HashObject.hashTree(trees, features, buckets);

        int deltaLevel = 1;
        if (original instanceof Delta) {
            deltaLevel = 1 + ((Delta) original).getDeltaLevel();
        }
        if (deltaLevel > 4) {
            trees = resolveAll(original.trees(), trees);
            features = resolveAll(original.features(), features);
            buckets = resolveAll(original.buckets(), buckets);
            return RevTreeBuilder.create(id, size, childTreeCount, trees, features, buckets);
        }

        return new RevTreeImplDelta(original, deltaLevel, id, size, childTreeCount, trees, features,
                buckets);
    }

    private static ImmutableSortedMap<Integer, Bucket> resolveAll(
            ImmutableSortedMap<Integer, Bucket> orig, ImmutableSortedMap<Integer, Bucket> buckets) {

        if (buckets == null) {
            return buckets;
        }
        ImmutableSortedMap.Builder<Integer, Bucket> builder = ImmutableSortedMap.naturalOrder();
        for (Entry<Integer, Bucket> e : buckets.entrySet()) {
            Integer index = e.getKey();
            Bucket b = e.getValue();
            if (b instanceof DeltaBucket) {
                b = orig.get(((DeltaBucket) b).index);
            }
            builder.put(index, b);
        }
        return builder.build();
    }

    private static ImmutableList<Node> resolveAll(ImmutableList<Node> orig,
            ImmutableList<Node> nodes) {
        if (nodes == null) {
            return nodes;
        }
        Set<String> names = new HashSet<>();
        Builder<Node> builder = ImmutableList.builder();
        for (Node node : nodes) {
            if (node instanceof DeltaNode) {
                node = orig.get(((DeltaNode) node).index);
            }
            if (names.contains(node.getName())) {
                throw new IllegalStateException("Duplicated node: " + node);
            }
            names.add(node.getName());
            builder.add(node);
        }
        return builder.build();
    }

    private ImmutableSortedMap<Integer, Bucket> delta(ImmutableSortedMap<Integer, Bucket> orig,
            @Nullable ImmutableSortedMap<Integer, Bucket> actual) {

        if (actual == null) {
            return ImmutableSortedMap.of();
        }

        ImmutableSortedMap.Builder<Integer, Bucket> builder = ImmutableSortedMap.naturalOrder();

        for (Entry<Integer, Bucket> entry : actual.entrySet()) {
            Integer index = entry.getKey();
            Bucket bucket = entry.getValue();
            Bucket origbucket = orig.get(index);
            if (bucket.equals(origbucket)) {
                bucket = new DeltaBucket(index);
                ((DeltaBucket) bucket).setTree(this);
            }
            builder.put(index, bucket);
        }

        return builder.build();
    }

    private ImmutableList<Node> delta(ImmutableList<Node> orig,
            @Nullable ImmutableList<Node> actual) {
        if (actual == null) {
            return ImmutableList.of();
        }

        Map<String, Node> origmap = new HashMap<>();
        try {
            orig.forEach((n) -> origmap.put(n.getName(), n));
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw e;
        }
        ImmutableList.Builder<Node> builder = ImmutableList.builder();

        for (int i = 0; i < actual.size(); i++) {
            Node node = actual.get(i);
            Node orignode = origmap.get(node.getName());
            if (node.equals(orignode)) {
                int index = orig.indexOf(orignode);
                node = new DeltaNode(node.getType(), index);
                ((DeltaNode) node).setTree(this);
            }

            builder.add(node);
        }

        return builder.build();
    }

    public static class DeltaNode extends Node {

        public final TYPE type;

        public final int index;

        private RevTreeImplDelta tree;

        public DeltaNode(TYPE type, int index) {
            this.type = type;
            this.index = index;
        }

        public void setTree(RevTreeImplDelta tree) {
            this.tree = tree;
        }

        public Node resolve() {
            if (null == tree) {
                throw new IllegalStateException("Tree not set for DeltaNode");
            }
            RevTree original = tree.original();
            ImmutableList<Node> subject = TYPE.FEATURE == type ? original.features()
                    : original.trees();
            Node n = subject.get(index);
            return n;
        }

        @Override
        public boolean intersects(Envelope env) {
            return resolve().intersects(env);
        }

        @Override
        public void expand(Envelope env) {
            resolve().expand(env);
        }

        @Override
        public Optional<Envelope> bounds() {
            return resolve().bounds();
        }

        @Override
        public TYPE getType() {
            return type;
        }

        @Override
        public String getName() {
            return resolve().getName();
        }

        @Override
        public ObjectId getObjectId() {
            return resolve().getObjectId();
        }

        @Override
        public Optional<ObjectId> getMetadataId() {
            return resolve().getMetadataId();
        }

        @Override
        public Map<String, Object> getExtraData() {
            return resolve().getExtraData();
        }
    }

    public static class DeltaBucket extends Bucket {

        public final Integer index;

        private RevTreeImplDelta tree;

        public DeltaBucket(Integer index) {
            this.index = index;
        }

        public void setTree(RevTreeImplDelta tree) {
            this.tree = tree;
        }

        public Bucket resolve() {
            return tree.original().buckets().get(index);
        }

        @Override
        public ObjectId getObjectId() {
            return resolve().getObjectId();
        }

        @Override
        public boolean intersects(Envelope env) {
            return resolve().intersects(env);
        }

        @Override
        public void expand(Envelope env) {
            resolve().expand(env);
        }

        @Override
        public Optional<Envelope> bounds() {
            return resolve().bounds();
        }
    }

}
