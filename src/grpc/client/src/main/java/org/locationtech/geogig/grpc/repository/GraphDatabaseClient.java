/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.grpc.repository;

import static org.locationtech.geogig.grpc.common.Utils.EMPTY;
import static org.locationtech.geogig.grpc.common.Utils.id;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.grpc.client.Stubs;
import org.locationtech.geogig.grpc.storage.EdgesQuery;
import org.locationtech.geogig.grpc.storage.GraphCommitMessage;
import org.locationtech.geogig.grpc.storage.GraphDatabaseGrpc.GraphDatabaseBlockingStub;
import org.locationtech.geogig.grpc.storage.GraphProperyMessage;
import org.locationtech.geogig.grpc.storage.ObjectIdMapping;
import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.repository.Hints;
import org.locationtech.geogig.repository.RepositoryConnectionException;
import org.locationtech.geogig.storage.GraphDatabase;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.protobuf.BoolValue;
import com.google.protobuf.Empty;
import com.google.protobuf.Int32Value;

import io.grpc.ManagedChannel;

public class GraphDatabaseClient implements GraphDatabase {

    private final Stubs stubs;

    private GraphDatabaseBlockingStub blockingStub;

    @Inject
    public GraphDatabaseClient(Hints hints) {
        this.stubs = Stubs.create(hints);
    }

    @VisibleForTesting
    public GraphDatabaseClient(ManagedChannel channel) {
        this.stubs = Stubs.create(channel);
    }

    @Override
    public void open() {
        if (stubs.open()) {
            blockingStub = stubs.newGraphDatabaseBlockingStub();
        }
    }

    @Override
    public boolean isOpen() {
        return stubs.isOpen();
    }

    @Override
    public void close() {
        stubs.close();
        blockingStub = null;
    }

    @Override
    public void configure() throws RepositoryConnectionException {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean checkConfig() throws RepositoryConnectionException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean exists(ObjectId commitId) {
        stubs.checkOpen();
        BoolValue exists = blockingStub.exists(id(commitId));
        return exists.getValue();
    }

    @Override
    public ImmutableList<ObjectId> getParents(ObjectId commitId) throws IllegalArgumentException {
        stubs.checkOpen();

        Iterator<org.locationtech.geogig.grpc.model.ObjectId> parents;
        parents = blockingStub.getParents(id(commitId));

        ImmutableList.Builder<ObjectId> builder = ImmutableList.builder();
        parents.forEachRemaining((id) -> builder.add(id(id)));
        return builder.build();
    }

    @Override
    public ImmutableList<ObjectId> getChildren(ObjectId commitId) throws IllegalArgumentException {
        stubs.checkOpen();

        Iterator<org.locationtech.geogig.grpc.model.ObjectId> children;
        children = blockingStub.getChildren(id(commitId));

        ImmutableList.Builder<ObjectId> builder = ImmutableList.builder();
        children.forEachRemaining((id) -> builder.add(id(id)));
        return builder.build();
    }

    @Override
    public boolean put(ObjectId commitId, ImmutableList<ObjectId> parentIds) {
        stubs.checkWritable();

        org.locationtech.geogig.grpc.storage.GraphCommitMessage.Builder builder = GraphCommitMessage
                .newBuilder();

        builder.setCommitId(id(commitId));
        parentIds.forEach((id) -> builder.addParentIds(id(id)));

        GraphCommitMessage request = builder.build();
        BoolValue ret = blockingStub.put(request);
        return ret.getValue();
    }

    @Override
    public void map(ObjectId mapped, ObjectId original) {
        stubs.checkWritable();
        org.locationtech.geogig.grpc.storage.ObjectIdMapping.Builder builder = ObjectIdMapping
                .newBuilder();
        builder.setMapped(id(mapped));
        builder.setOriginal(id(original));
        ObjectIdMapping request = builder.build();
        Empty ret = blockingStub.map(request);
    }

    @Override
    @Nullable
    public ObjectId getMapping(ObjectId commitId) {
        stubs.checkOpen();

        org.locationtech.geogig.grpc.model.ObjectId mapping = blockingStub.getMapping(id(commitId));
        @Nullable
        ObjectId id = id(mapping);
        return id;
    }

    @Override
    public int getDepth(ObjectId commitId) {
        stubs.checkOpen();
        Int32Value depth = blockingStub.getDepth(id(commitId));
        return depth.getValue();
    }

    @Override
    public void setProperty(ObjectId commitId, String propertyName, String propertyValue) {
        stubs.checkWritable();
        org.locationtech.geogig.grpc.storage.GraphProperyMessage.Builder builder = GraphProperyMessage
                .newBuilder();
        builder.setCommitId(id(commitId));
        builder.setPropertyName(propertyName);
        builder.setPropertyValue(propertyValue);
        GraphProperyMessage message = builder.build();
        Empty ret = blockingStub.setProperty(message);
    }

    @Override
    public GraphNode getNode(ObjectId id) {
        stubs.checkOpen();

        org.locationtech.geogig.grpc.storage.GraphNode node = blockingStub.getNode(id(id));
        return new RemoteGraphNode(node);
    }

    @Override
    public void truncate() {
        stubs.checkWritable();
        Empty ret = blockingStub.truncate(EMPTY);
    }

    private class RemoteGraphNode extends GraphNode {

        private ObjectId id;

        private boolean sparse;

        public RemoteGraphNode(org.locationtech.geogig.grpc.storage.GraphNode rpcNode) {
            this.id = id(rpcNode.getIdentifier());
            this.sparse = rpcNode.getSparse();
        }

        @Override
        public ObjectId getIdentifier() {
            return id;
        }

        @Override
        public Iterator<GraphEdge> getEdges(final Direction direction) {

            org.locationtech.geogig.grpc.storage.Direction rpcDirection = org.locationtech.geogig.grpc.storage.Direction
                    .forNumber(direction.ordinal());

            GraphDatabaseBlockingStub blockingStub = GraphDatabaseClient.this.blockingStub;

            EdgesQuery query = EdgesQuery.newBuilder()//
                    .setDirection(rpcDirection)//
                    .setIdentifier(id(this.id)).build();

            Iterator<org.locationtech.geogig.grpc.storage.GraphEdge> rpcEdges;
            rpcEdges = blockingStub.getEdges(query);

            List<GraphEdge> edges = new LinkedList<GraphEdge>();
            rpcEdges.forEachRemaining((e) -> edges.add(newEdge(e)));
            return edges.iterator();
        }

        private GraphEdge newEdge(org.locationtech.geogig.grpc.storage.GraphEdge e) {

            org.locationtech.geogig.grpc.storage.GraphNode from = e.getFrom();
            org.locationtech.geogig.grpc.storage.GraphNode to = e.getTo();

            GraphEdge edge = new GraphEdge(new RemoteGraphNode(from), new RemoteGraphNode(to));
            return edge;
        }

        @Override
        public boolean isSparse() {
            return sparse;
        }
    }

}
