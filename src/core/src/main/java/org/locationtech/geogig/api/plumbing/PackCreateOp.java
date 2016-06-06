package org.locationtech.geogig.api.plumbing;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.model.Bucket;
import org.locationtech.geogig.model.Node;
import org.locationtech.geogig.model.NodeRef;
import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.RevFeatureType;
import org.locationtech.geogig.model.RevObject;
import org.locationtech.geogig.model.RevTree;
import org.locationtech.geogig.plumbing.diff.PostOrderDiffWalk;
import org.locationtech.geogig.plumbing.diff.PostOrderDiffWalk.Consumer;
import org.locationtech.geogig.plumbing.diff.PreOrderDiffWalk.BucketIndex;
import org.locationtech.geogig.repository.AbstractGeoGigOp;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.locationtech.geogig.storage.ObjectStore;
import org.locationtech.geogig.storage.datastream.DataStreamSerializationFactoryV2;
import org.locationtech.geogig.storage.impl.ObjectSerializingFactory;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;

public class PackCreateOp extends AbstractGeoGigOp<Void> {

    private Supplier<OutputStream> out;

    private ObjectId oldTree;

    private ObjectId newTree;

    private ObjectSerializingFactory serializer = DataStreamSerializationFactoryV2.INSTANCE;

    public PackCreateOp setOutput(Supplier<OutputStream> out) {
        this.out = out;
        return this;
    }

    public PackCreateOp setOldTree(ObjectId oldTree) {
        this.oldTree = oldTree;
        return this;
    }

    public PackCreateOp setNewTree(ObjectId newTree) {
        this.newTree = newTree;
        return this;
    }

    public PackCreateOp setSerializer(ObjectSerializingFactory serializer) {
        this.serializer = serializer;
        return this;
    }

    @Override
    protected Void _call() {
        checkArgument(out != null, "output stream not supplied");
        OutputStream out = this.out.get();
        checkNotNull(out, "output stream is null");
        checkArgument(oldTree != null, "old tree not supplied");
        checkArgument(newTree != null, "new tree not supplied");

        ObjectDatabase objects = objectDatabase();
        RevTree left = objects.getTree(oldTree);
        RevTree right = objects.getTree(newTree);
        PostOrderDiffWalk walk = new PostOrderDiffWalk(left, right, objects, objects);

        try (Packer packer = new Packer(out, serializer, objects)) {
            WriterConsumer consumer = new WriterConsumer(packer);
            walk.walk(consumer);
        }
        return null;
    }

    private static class Packer implements AutoCloseable {

        private OutputStream out;

        private final ObjectSerializingFactory serializer;

        private Set<ObjectId> ids = new HashSet<>();

        private static final int WRITE_THRESHOLD = 1000;

        private ObjectStore source;

        private Set<ObjectId> writtenFeatureTypes = new HashSet<>();

        Packer(OutputStream out, ObjectSerializingFactory serializer, ObjectStore source) {
            this.out = out;
            this.serializer = serializer;
            this.source = source;
        }

        public void close() {
            if (!ids.isEmpty()) {
                save(ids);
            }
        }

        public synchronized void add(ObjectId objectId) {
            ids.add(objectId);
            if (ids.size() == WRITE_THRESHOLD) {
                save(ids);
                ids.clear();
            }
        }

        private void save(Set<ObjectId> ids) {
            Iterator<RevObject> it = source.getAll(ids);
            try {
                byte[] id = new byte[ObjectId.NUM_BYTES];
                while (it.hasNext()) {
                    RevObject obj = it.next();
                    if (obj instanceof RevFeatureType) {
                        if (writtenFeatureTypes.contains(obj.getId())) {
                            continue;
                        }
                        writtenFeatureTypes.add(obj.getId());
                    }
                    obj.getId().getRawValue(id);
                    out.write(id);
                    serializer.write(obj, out);
                }
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private static class WriterConsumer implements Consumer {

        private final Packer packer;

        WriterConsumer(final Packer packer) {
            this.packer = packer;
        }

        @Override
        public void feature(@Nullable NodeRef left, @Nullable NodeRef right) {
            addNode(left);
            addNode(right);
        }

        @Override
        public void tree(@Nullable NodeRef left, @Nullable NodeRef right) {
            addNode(left);
            addNode(right);
        }

        private void addNode(@Nullable NodeRef nodeRef) {
            if (nodeRef != null) {
                Node node = nodeRef.getNode();
                Optional<ObjectId> metadataId = node.getMetadataId();
                ObjectId objectId = node.getObjectId();
                packer.add(objectId);
                if (metadataId.isPresent()) {
                    packer.add(metadataId.get());
                }
            }
        }

        @Override
        public void bucket(@Nullable NodeRef leftParent, @Nullable NodeRef rightParent,
                BucketIndex bucketIndex, @Nullable Bucket left, @Nullable Bucket right) {

            if (left != null) {
                packer.add(left.getObjectId());
            }
            if (right != null) {
                packer.add(right.getObjectId());
            }
        }

    }
}
