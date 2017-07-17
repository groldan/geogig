/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.grpc.server;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.locationtech.geogig.grpc.common.Utils.EMPTY;
import static org.locationtech.geogig.grpc.common.Utils.id;

import java.util.Iterator;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.grpc.storage.GraphDatabaseGrpc.GraphDatabaseImplBase;
import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.storage.GraphDatabase;
import org.locationtech.geogig.storage.GraphDatabase.Direction;
import org.locationtech.geogig.storage.GraphDatabase.GraphEdge;
import org.locationtech.geogig.storage.GraphDatabase.GraphNode;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.BoolValue;
import com.google.protobuf.Empty;
import com.google.protobuf.Int32Value;

public class GraphDatabaseService extends GraphDatabaseImplBase {

    private final Supplier<GraphDatabase> _localdb;

    @VisibleForTesting
    public GraphDatabaseService(GraphDatabase localdb) {
        this(Suppliers.ofInstance(localdb));
    }

    public GraphDatabaseService(Supplier<GraphDatabase> localdb) {
        checkNotNull(localdb);
        this._localdb = localdb;
    }

    private GraphDatabase localdb() {
        GraphDatabase database = _localdb.get();
        checkNotNull(database);
        return database;
    }

    @Override
    public void exists(org.locationtech.geogig.grpc.model.ObjectId request,
            io.grpc.stub.StreamObserver<BoolValue> responseObserver) {
        try {
            boolean exists = localdb().exists(id(request));
            responseObserver.onNext(BoolValue.newBuilder().setValue(exists).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getParents(org.locationtech.geogig.grpc.model.ObjectId request,
            io.grpc.stub.StreamObserver<org.locationtech.geogig.grpc.model.ObjectId> responseObserver) {
        try {
            ImmutableList<ObjectId> parents = localdb().getParents(id(request));
            parents.forEach((parent) -> responseObserver.onNext(id(parent)));
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getChildren(org.locationtech.geogig.grpc.model.ObjectId request,
            io.grpc.stub.StreamObserver<org.locationtech.geogig.grpc.model.ObjectId> responseObserver) {
        try {

            ImmutableList<ObjectId> children = localdb().getChildren(id(request));
            children.forEach((child) -> responseObserver.onNext(id(child)));
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void put(org.locationtech.geogig.grpc.storage.GraphCommitMessage request,
            io.grpc.stub.StreamObserver<BoolValue> responseObserver) {
        try {

            ObjectId commitId = id(request.getCommitId());
            ImmutableList.Builder<ObjectId> parentIds = ImmutableList.builder();
            request.getParentIdsList().forEach((parent) -> parentIds.add(id(parent)));
            boolean ret = localdb().put(commitId, parentIds.build());
            responseObserver.onNext(BoolValue.newBuilder().setValue(ret).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    public void map(org.locationtech.geogig.grpc.storage.ObjectIdMapping request,
            io.grpc.stub.StreamObserver<Empty> responseObserver) {
        try {

            localdb().map(id(request.getMapped()), id(request.getOriginal()));
            responseObserver.onNext(EMPTY);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getMapping(org.locationtech.geogig.grpc.model.ObjectId request,
            io.grpc.stub.StreamObserver<org.locationtech.geogig.grpc.model.ObjectId> responseObserver) {
        try {
            @Nullable
            ObjectId mapping = localdb().getMapping(id(request));
            responseObserver.onNext(id(mapping));
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getDepth(org.locationtech.geogig.grpc.model.ObjectId request,
            io.grpc.stub.StreamObserver<Int32Value> responseObserver) {
        try {
            int depth = localdb().getDepth(id(request));
            responseObserver.onNext(Int32Value.newBuilder().setValue(depth).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void setProperty(org.locationtech.geogig.grpc.storage.GraphProperyMessage request,
            io.grpc.stub.StreamObserver<Empty> responseObserver) {
        try {
            ObjectId id = id(request.getCommitId());
            String propertyName = request.getPropertyName();
            String propertyValue = request.getPropertyValue();
            localdb().setProperty(id, propertyName, propertyValue);
            responseObserver.onNext(EMPTY);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getNode(org.locationtech.geogig.grpc.model.ObjectId request,
            io.grpc.stub.StreamObserver<org.locationtech.geogig.grpc.storage.GraphNode> responseObserver) {
        try {
            GraphNode node = localdb().getNode(id(request));
            org.locationtech.geogig.grpc.storage.GraphNode value = newNode(node);
            responseObserver.onNext(value);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    private org.locationtech.geogig.grpc.storage.GraphNode newNode(GraphNode node) {
        boolean sparse = node.isSparse();
        ObjectId identifier = node.getIdentifier();
        org.locationtech.geogig.grpc.storage.GraphNode value = org.locationtech.geogig.grpc.storage.GraphNode
                .newBuilder()//
                .setIdentifier(id(identifier))//
                .setSparse(sparse)//
                .build();
        return value;
    }

    @Override
    public void getEdges(org.locationtech.geogig.grpc.storage.EdgesQuery request,
            io.grpc.stub.StreamObserver<org.locationtech.geogig.grpc.storage.GraphEdge> responseObserver) {
        try {
            ObjectId identifier = id(request.getIdentifier());
            org.locationtech.geogig.grpc.storage.Direction rpcDirection = request.getDirection();
            GraphNode node = localdb().getNode(identifier);
            Direction direction = Direction.values()[rpcDirection.getNumber()];
            Iterator<GraphEdge> edges = node.getEdges(direction);

            edges.forEachRemaining((e) -> responseObserver.onNext(newEdge(e)));
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    private org.locationtech.geogig.grpc.storage.GraphEdge newEdge(GraphEdge e) {
        GraphNode from = e.getFromNode();
        GraphNode to = e.getToNode();

        org.locationtech.geogig.grpc.storage.GraphNode fromNode = newNode(from);
        org.locationtech.geogig.grpc.storage.GraphNode toNode = newNode(to);
        org.locationtech.geogig.grpc.storage.GraphEdge edge;
        edge = org.locationtech.geogig.grpc.storage.GraphEdge.newBuilder()//
                .setFrom(fromNode)//
                .setTo(toNode)//
                .build();
        return edge;
    }

    @Override
    public void truncate(Empty request, io.grpc.stub.StreamObserver<Empty> responseObserver) {
        try {
            localdb().truncate();
            responseObserver.onNext(EMPTY);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}
