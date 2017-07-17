/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.grpc.common;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.grpc.StringTuple;
import org.locationtech.geogig.grpc.model.Ref;
import org.locationtech.geogig.grpc.model.RevObject.TYPE;
import org.locationtech.geogig.grpc.storage.BlobMessage;
import org.locationtech.geogig.grpc.storage.ConflictMessage;
import org.locationtech.geogig.grpc.storage.ObjectInfo;
import org.locationtech.geogig.model.Node;
import org.locationtech.geogig.model.NodeRef;
import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.RevFeature;
import org.locationtech.geogig.model.RevObject;
import org.locationtech.geogig.model.RevTree;
import org.locationtech.geogig.repository.Conflict;
import org.locationtech.geogig.repository.IndexInfo;
import org.locationtech.geogig.repository.impl.SpatialOps;
import org.locationtech.geogig.storage.datastream.v2_3.DataStreamSerializationFactoryV2_3;
import org.locationtech.geogig.storage.impl.IndexInfoSerializer;
import org.locationtech.geogig.storage.impl.ObjectSerializingFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.StringValue;
import com.vividsolutions.jts.geom.Envelope;

public class Utils {
    public static final Empty EMPTY = Empty.newBuilder().build();

    private static final ObjectSerializingFactory serializer = DataStreamSerializationFactoryV2_3.INSTANCE;

    public static Map<String, String> toMap(Iterator<Ref> all) {
        Map<String, String> result = new HashMap<>();
        all.forEachRemaining((r) -> result.put(r.getName(), r.getValue()));
        return result;
    }

    public static StringValue str(@Nullable String value) {
        StringValue.Builder builder = StringValue.newBuilder();
        if (value != null) {
            builder.setValue(value);
        }
        return builder.build();
    }

    public static Ref ref(@Nullable String name, @Nullable String value, boolean symref) {
        org.locationtech.geogig.grpc.model.Ref.Builder builder = Ref.newBuilder();
        if (name != null) {
            builder.setName(name);
        }
        if (value != null) {
            builder.setValue(value);
        }
        builder.setSymref(symref);
        return builder.build();
    }

    public static StringTuple tuple(String k, String v) {
        org.locationtech.geogig.grpc.StringTuple.Builder builder = StringTuple.newBuilder();

        if (k != null) {
            builder.setKey(k);
        }
        if (v != null) {
            builder.setValue(v);
        }
        return builder.build();
    }

    public static @Nullable ObjectId id(org.locationtech.geogig.grpc.model.ObjectId rpcId) {
        if (rpcId.getHash().isEmpty()) {
            return null;
        }
        byte[] buff = new byte[ObjectId.NUM_BYTES];
        rpcId.getHash().copyTo(buff, 0);
        return ObjectId.createNoClone(buff);
    }

    public static org.locationtech.geogig.grpc.model.ObjectId id(@Nullable ObjectId id) {

        org.locationtech.geogig.grpc.model.ObjectId.Builder builder = org.locationtech.geogig.grpc.model.ObjectId
                .newBuilder();
        if (id != null) {
            builder.setHash(ByteString.copyFrom(id.getRawValue()));
        }
        return builder.build();
    }

    public static @Nullable RevObject object(
            org.locationtech.geogig.grpc.model.RevObject rpcObject) {

        org.locationtech.geogig.grpc.model.ObjectId id = rpcObject.getId();
        ByteString serialForm = rpcObject.getSerialForm();
        if (serialForm.isEmpty()) {
            return null;
        }
        RevObject revObject;
        try {
            ObjectId oid = id(id);
            byte[] buff = serialForm.toByteArray();
            revObject = serializer.read(oid, buff, 0, buff.length);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return revObject;
    }

    public static org.locationtech.geogig.grpc.model.RevObject object(RevObject revObject) {
        Preconditions.checkNotNull(revObject);
        return object(revObject.getId(), revObject);
    }

    public static org.locationtech.geogig.grpc.model.RevObject object(ObjectId id,
            @Nullable RevObject revObject) {

        org.locationtech.geogig.grpc.model.RevObject.Builder builder = org.locationtech.geogig.grpc.model.RevObject
                .newBuilder();

        builder.setId(id(id));
        if (revObject != null) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try {
                serializer.write(revObject, out);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
            builder.setType(TYPE.forNumber(revObject.getType().ordinal()));
            ByteString serialForm = ByteString.copyFrom(out.toByteArray());
            builder.setSerialForm(serialForm);
        }
        return builder.build();
    }

    public static ObjectInfo queryObject(NodeRef ref) {
        String path = ref.path();
        ObjectId objectId = ref.getObjectId();
        ObjectId metadataId = ref.getMetadataId();
        org.locationtech.geogig.grpc.storage.ObjectInfo.Builder builder = ObjectInfo.newBuilder();

        builder.setPath(path);
        builder.setObject(object(objectId, null));
        if (!ObjectId.NULL.equals(metadataId)) {
            builder.setMetadataId(id(metadataId));
        }
        return builder.build();
    }

    public static NodeRef queryObjectToRef(
            org.locationtech.geogig.grpc.storage.ObjectInfo queryObject) {

        String path = queryObject.getPath();
        org.locationtech.geogig.grpc.model.ObjectId queryId = queryObject.getObject().getId();
        org.locationtech.geogig.grpc.model.ObjectId metadataId = queryObject.getMetadataId();

        ObjectId id = id(queryId);
        ObjectId mdid = metadataId.isInitialized() ? id(metadataId) : ObjectId.NULL;

        String parentPath = NodeRef.parentPath(path);
        String name = NodeRef.nodeFromPath(path);

        Node node = Node.create(name, id, mdid,
                org.locationtech.geogig.model.RevObject.TYPE.FEATURE, null);

        NodeRef ref = NodeRef.create(parentPath, node);
        return ref;
    }

    public static org.locationtech.geogig.grpc.storage.ObjectInfo objectToMessage(
            org.locationtech.geogig.storage.ObjectInfo<RevObject> response) {

        org.locationtech.geogig.grpc.storage.ObjectInfo.Builder builder;
        builder = org.locationtech.geogig.grpc.storage.ObjectInfo.newBuilder();

        String path = response.ref().path();

        org.locationtech.geogig.grpc.model.RevObject object = object(response.object());

        builder.setPath(path);
        builder.setObject(object);
        if (!response.ref().getMetadataId().isNull()) {
            org.locationtech.geogig.grpc.model.ObjectId metadataId = id(
                    response.ref().getMetadataId());
            builder.setMetadataId(metadataId);
        }

        return builder.build();

    }

    public static org.locationtech.geogig.storage.ObjectInfo<RevObject> messageToObject(
            ObjectInfo result) {

        String path = result.getPath();
        org.locationtech.geogig.grpc.model.RevObject object = result.getObject();
        org.locationtech.geogig.grpc.model.ObjectId metadataId = result.getMetadataId();

        ObjectId objectId;
        ObjectId mdid = metadataId.isInitialized() ? id(metadataId) : ObjectId.NULL;

        @Nullable
        RevObject revObject = object(object);
        Preconditions.checkArgument(revObject != null);

        objectId = revObject.getId();

        String parentPath = NodeRef.parentPath(path);
        String name = NodeRef.nodeFromPath(path);

        Envelope bounds = null;
        if (revObject instanceof RevFeature) {
            bounds = SpatialOps.boundsOf((RevFeature) revObject);
        } else if (revObject instanceof RevTree) {
            bounds = SpatialOps.boundsOf((RevTree) revObject);
        }

        Node node = Node.create(name, objectId, mdid, revObject.getType(), bounds);
        NodeRef ref = NodeRef.create(parentPath, node);
        org.locationtech.geogig.storage.ObjectInfo<RevObject> objectInfo;
        objectInfo = org.locationtech.geogig.storage.ObjectInfo.of(ref, revObject);
        return objectInfo;
    }

    public static @Nullable Conflict toConflict(
            org.locationtech.geogig.grpc.storage.Conflict rpcconflict) {
        Conflict c = null;
        if (!Strings.isNullOrEmpty(rpcconflict.getPath())) {
            String path = rpcconflict.getPath();
            ObjectId ancestor = id(rpcconflict.getAncestor());
            ObjectId ours = id(rpcconflict.getOurs());
            ObjectId theirs = id(rpcconflict.getTheirs());
            c = new Conflict(path, ancestor, ours, theirs);
        }
        return c;
    }

    public static org.locationtech.geogig.grpc.storage.Conflict toRpcConflict(
            @Nullable Conflict c) {
        org.locationtech.geogig.grpc.storage.Conflict.Builder builder;
        builder = org.locationtech.geogig.grpc.storage.Conflict.newBuilder();
        if (c != null) {
            builder.setPath(c.getPath());
            builder.setAncestor(id(c.getAncestor()));
            builder.setTheirs(id(c.getTheirs()));
            builder.setOurs(id(c.getOurs()));
        }
        return builder.build();
    }

    public static ConflictMessage toConflictMessage(@Nullable String namespace, Conflict c) {
        org.locationtech.geogig.grpc.storage.ConflictMessage.Builder builder = ConflictMessage
                .newBuilder();
        if (null != namespace) {
            builder.setNamespace(namespace);
        }
        builder.setConflict(toRpcConflict(c));
        return builder.build();
    }

    public static BlobMessage newBlob(@Nullable String namespace, String path,
            @Nullable byte[] bs) {
        org.locationtech.geogig.grpc.storage.BlobMessage.Builder builder = BlobMessage.newBuilder();
        if (namespace != null) {
            builder.setNamespace(namespace);
        }
        builder.setPath(path);
        if (bs != null) {
            builder.setContent(ByteString.copyFrom(bs));
            builder.setFound(true);
        }
        return builder.build();
    }

    public static org.locationtech.geogig.grpc.storage.IndexInfo newRpcIndexInfo(IndexInfo info) {
        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        IndexInfoSerializer.serialize(info, out);
        org.locationtech.geogig.grpc.storage.IndexInfo request = org.locationtech.geogig.grpc.storage.IndexInfo//
                .newBuilder()//
                .setSerialForm(ByteString.copyFrom(out.toByteArray()))//
                .build();
        return request;
    }

    public static IndexInfo newIndexInfo(org.locationtech.geogig.grpc.storage.IndexInfo rpcInfo) {
        byte[] seralForm = rpcInfo.getSerialForm().toByteArray();
        IndexInfo info = IndexInfoSerializer.deserialize(ByteStreams.newDataInput(seralForm));
        return info;
    }
}
