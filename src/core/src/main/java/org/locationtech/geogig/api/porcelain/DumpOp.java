package org.locationtech.geogig.api.porcelain;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.api.plumbing.FindOrphanCommits;
import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.Ref;
import org.locationtech.geogig.model.RevObject;
import org.locationtech.geogig.model.RevObject.TYPE;
import org.locationtech.geogig.model.SymRef;
import org.locationtech.geogig.plumbing.DiffTree;
import org.locationtech.geogig.plumbing.RefParse;
import org.locationtech.geogig.porcelain.BranchListOp;
import org.locationtech.geogig.repository.AbstractGeoGigOp;
import org.locationtech.geogig.repository.DiffEntry;
import org.locationtech.geogig.repository.DiffEntry.ChangeType;
import org.locationtech.geogig.storage.GraphDatabase;
import org.locationtech.geogig.storage.GraphDatabase.Direction;
import org.locationtech.geogig.storage.GraphDatabase.GraphEdge;
import org.locationtech.geogig.storage.GraphDatabase.GraphNode;
import org.locationtech.geogig.storage.datastream.DataStreamSerializationFactoryV2;
import org.locationtech.geogig.storage.datastream.Varint;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;

public class DumpOp extends AbstractGeoGigOp<Void> {

    private Supplier<OutputStream> out = Suppliers.ofInstance(System.out);

    public DumpOp setOutput(Supplier<OutputStream> out) {
        this.out = out;
        return this;
    }

    @Override
    protected Void _call() {
        final Set<ObjectId> orhpanCommits = command(FindOrphanCommits.class).call();
        final ImmutableList<Ref> tips = command(BranchListOp.class).call();
        final DataOutputStream out = new DataOutputStream(this.out.get());

        try {
            writeHeader(out);
            REFS.write(tips, out);

            Set<GraphDatabase.GraphEdge> visitedCommits = new HashSet<>();

            long totalObjects = 0;

            for (Ref tip : tips) {
                totalObjects += dump(tip.getObjectId(), out, visitedCommits);
            }

            writeFooter(totalObjects, out);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }

        return null;
    }

    private void writeFooter(long totalObjects, DataOutputStream out) throws IOException {
        out.writeLong(totalObjects);
        out.flush();
    }

    /**
     * @param out
     */
    private void writeHeader(OutputStream out) {
        Optional<Ref> head = command(RefParse.class).setName(Ref.HEAD).call();
        Preconditions.checkState(head.isPresent());
        Ref ref = head.get();
        if (ref instanceof SymRef) {

        }
    }

    private long dump(final ObjectId commitId, DataOutputStream out, Set<GraphEdge> visitedCommits)
            throws IOException {
        GraphDatabase graph = graphDatabase();
        GraphNode node = graph.getNode(commitId);
        Iterator<GraphEdge> parents = node.getEdges(Direction.OUT);
        long objCount = 0;
        while (parents.hasNext()) {
            GraphEdge parentToThis = parents.next();
            Preconditions.checkState(commitId.equals(parentToThis.getFromNode().getIdentifier()));
            if (!visitedCommits.contains(parentToThis)) {
                objCount += dumpDiff(parentToThis.getToNode().getIdentifier(), commitId, out);
                visitedCommits.add(parentToThis);
            }
        }
        return objCount;
    }

    private long dumpDiff(ObjectId from, ObjectId to, DataOutputStream out) throws IOException {
        Iterator<DiffEntry> diffs = command(DiffTree.class).setOldVersion(from.toString())
                .setNewVersion(to.toString()).setReportTrees(true).call();

        int partitionSize = 1_000;
        Iterator<List<DiffEntry>> partitions = Iterators.partition(diffs, partitionSize);

        long count = 0;
        while (partitions.hasNext()) {
            List<DiffEntry> p = partitions.next();
            Map<ObjectId, RevObject> objects = getNew(p);
            for (DiffEntry de : p) {
                RevObject o = null;
                ObjectId metatadaId;
                if (!de.isDelete()) {
                    ObjectId newObjectId = de.newObjectId();
                    metatadaId = de.getNewObject().getNode().getMetadataId().orNull();
                    o = objects.get(newObjectId);
                    Preconditions.checkState(o != null,
                            "Object not found: " + newObjectId + " for diff: " + de);
                }
                ChangeType type = de.changeType();
                String path = de.isDelete() ? de.oldPath() : de.newPath();
                Diff diff = new Diff(type, path, o);
                DIFF.write(diff, out);
                count++;
            }
        }
        return count;
    }

    private Map<ObjectId, RevObject> getNew(List<DiffEntry> p) {
        Set<ObjectId> ids = Sets.newHashSet(Iterables.filter(Iterables.transform(p, (de) -> {
            if (de.isDelete()) {
                return null;
            }
            if (de.getNewObject().getType().equals(TYPE.TREE)) {
                ObjectId newMdId = de.getNewObject().getMetadataId();
                ObjectId oldMdId = de.getOldObject().getMetadataId();
                if (!newMdId.equals(oldMdId)) {
                    return de.getNewObject().getMetadataId();
                }
                return ObjectId.NULL;
            }
            return de.newObjectId();
        }), Predicates.notNull()));

        Iterator<RevObject> objs = objectDatabase().getAll(ids);

        return Maps.uniqueIndex(objs, (o) -> o.getId());
    }

    private static interface Serializer<T> {

        public void write(T obj, DataOutputStream out) throws IOException;

        public T read(DataInputStream in) throws IOException;
    }

    private static class Diff {

        final ChangeType changeType;

        final String path;

        final RevObject newObject;

        public Diff(ChangeType type, String path, @Nullable RevObject newObject) {
            this.changeType = type;
            this.path = path;
            this.newObject = newObject;
        }

    }

    private static final Serializer<Diff> DIFF = new Serializer<DumpOp.Diff>() {

        @Override
        public void write(Diff obj, DataOutputStream out) throws IOException {
            String path = obj.path;
            ChangeType type = obj.changeType;
            RevObject object = obj.newObject;

            out.writeByte(type.value());
            out.writeUTF(path);
            if (object != null) {
                OID.write(object.getId(), out);
                DataStreamSerializationFactoryV2.INSTANCE.write(object, out);
            }
            out.flush();
        }

        @Override
        public Diff read(DataInputStream in) throws IOException {
            ChangeType changeType = ChangeType.valueOf(in.readByte() & 0xFF);
            String path = in.readUTF();
            RevObject obj = null;
            if (!changeType.equals(ChangeType.REMOVED)) {
                ObjectId id = OID.read(in);
                obj = DataStreamSerializationFactoryV2.INSTANCE.read(id, in);
            }

            Diff diff = new Diff(changeType, path, obj);
            return diff;
        }
    };

    private static final Serializer<ObjectId> OID = new Serializer<ObjectId>() {

        private byte[] bytes = new byte[ObjectId.NUM_BYTES];

        @Override
        public synchronized void write(ObjectId obj, DataOutputStream out) throws IOException {
            obj.getRawValue(bytes);
            out.write(bytes);
        }

        @Override
        public ObjectId read(DataInputStream in) throws IOException {
            byte[] bytes = new byte[ObjectId.NUM_BYTES];
            ByteStreams.readFully(in, bytes);
            return ObjectId.createNoClone(bytes);
        }
    };

    private static Serializer<List<Ref>> REFS = new Serializer<List<Ref>>() {

        @Override
        public void write(List<Ref> obj, DataOutputStream out) throws IOException {
            Varint.writeUnsignedVarInt(obj.size(), out);
            for (Ref ref : obj) {
                String name = ref.getName();
                ObjectId objectId = ref.getObjectId();
                out.writeUTF(name);
                OID.write(objectId, out);
            }
            out.flush();
        }

        @Override
        public List<Ref> read(DataInputStream in) throws IOException {
            final int size = Varint.readUnsignedVarInt(in);
            List<Ref> refs = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                String name = in.readUTF();
                ObjectId oid = OID.read(in);
                refs.add(new Ref(name, oid));
            }
            return refs;
        }
    };
}
