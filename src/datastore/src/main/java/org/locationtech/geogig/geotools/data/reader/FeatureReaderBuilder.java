/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.geotools.data.reader;

import static com.google.common.base.Optional.absent;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.notNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.eclipse.jdt.annotation.Nullable;
import org.geotools.data.DataUtilities;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.SchemaException;
import org.geotools.filter.spatial.ReprojectingFilterVisitor;
import org.geotools.filter.visitor.SimplifyingFilterVisitor;
import org.geotools.filter.visitor.SpatialFilterVisitor;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.crs.DefaultEngineeringCRS;
import org.geotools.renderer.ScreenMap;
import org.locationtech.geogig.data.retrieve.BulkFeatureRetriever;
import org.locationtech.geogig.geotools.data.GeoGigDataStore.ChangeType;
import org.locationtech.geogig.model.Bounded;
import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.Ref;
import org.locationtech.geogig.model.RevTree;
import org.locationtech.geogig.plumbing.DiffTree;
import org.locationtech.geogig.plumbing.DiffTree.Stats;
import org.locationtech.geogig.plumbing.FindTreeChild;
import org.locationtech.geogig.plumbing.ResolveTreeish;
import org.locationtech.geogig.porcelain.index.Index;
import org.locationtech.geogig.repository.AutoCloseableIterator;
import org.locationtech.geogig.repository.Context;
import org.locationtech.geogig.repository.DiffEntry;
import org.locationtech.geogig.repository.IndexInfo;
import org.locationtech.geogig.repository.NodeRef;
import org.locationtech.geogig.repository.impl.SpatialOps;
import org.locationtech.geogig.storage.IndexDatabase;
import org.locationtech.geogig.storage.ObjectStore;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.Id;
import org.opengis.filter.identity.FeatureId;
import org.opengis.filter.identity.Identifier;
import org.opengis.filter.sort.SortBy;
import org.opengis.filter.spatial.BBOX;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.impl.PackedCoordinateSequenceFactory;

/**
 * A builder object for a {@link FeatureReader} that fetches data from a geogig {@link RevTree}
 * representing a "layer".
 * <p>
 * 
 *
 */
public class FeatureReaderBuilder {

    private static final GeometryFactory DEFAULT_GEOMETRY_FACTORY = new GeometryFactory(
            new PackedCoordinateSequenceFactory());

    // cache filter factory to avoid the overhead of repeated calls to
    // CommonFactoryFinder.getFilterFactory2
    private static final FilterFactory2 filterFactory = CommonFactoryFinder.getFilterFactory2();

    private final Context repo;

    private final SimpleFeatureType fullSchema;

    private final Set<String> fullSchemaAttributeNames;

    private @Nullable String oldHeadRef;

    private String headRef = Ref.HEAD;

    private Filter filter = Filter.INCLUDE;

    private @Nullable String[] outputSchemaPropertyNames = Query.ALL_NAMES;

    private @Nullable ScreenMap screenMap;

    private @Nullable SortBy[] sortBy;

    private @Nullable Integer limit;

    private @Nullable Integer offset;

    private ChangeType changeType = ChangeType.ADDED;

    private GeometryFactory geometryFactory = DEFAULT_GEOMETRY_FACTORY;

    private NodeRef typeRef;

    private boolean ignoreIndex;

    public FeatureReaderBuilder(Context repo, SimpleFeatureType fullSchema, NodeRef typeRef) {
        this.repo = repo;
        this.fullSchema = fullSchema;
        this.typeRef = typeRef;
        this.fullSchemaAttributeNames = Sets.newHashSet(
                Lists.transform(fullSchema.getAttributeDescriptors(), (a) -> a.getLocalName()));
    }

    /**
     * Sets the {@code ignoreIndex} flag to true, indicating that a spatial index lookup will be
     * ignored and only the canonical tree will be used by the returned feature reader.
     * <p>
     * This is particularly useful if the reader is constructed out of a diff between two version of
     * a featuretype tree (e.g. both {@link #oldHeadRef(String)} and {@link #headRef(String)} have
     * been provided), in order for the {@link DiffTree} op used to create the feature stream to
     * accurately represent changes, since most of the time an index reports changes as two separate
     * events for the removal and addition of the feature instead of one single event for the
     * change.
     */
    public FeatureReaderBuilder ignoreIndex() {
        this.ignoreIndex = true;
        return this;
    }

    public static FeatureReaderBuilder builder(Context repo, SimpleFeatureType fullSchema,
            NodeRef typeRef) {
        return new FeatureReaderBuilder(repo, fullSchema, typeRef);
    }

    public FeatureReaderBuilder oldHeadRef(@Nullable String oldHeadRef) {
        this.oldHeadRef = oldHeadRef;
        return this;
    }

    public FeatureReaderBuilder changeType(ChangeType changeType) {
        checkNotNull(changeType);
        this.changeType = changeType;
        return this;
    }

    public FeatureReaderBuilder headRef(String headRef) {
        checkNotNull(headRef);
        this.headRef = headRef;
        return this;
    }

    /**
     * @param propertyNames which property names to include as the output schema, {@code null} means
     *        all properties
     */
    public FeatureReaderBuilder propertyNames(@Nullable String... propertyNames) {
        this.outputSchemaPropertyNames = propertyNames;
        return this;
    }

    public FeatureReaderBuilder filter(Filter filter) {
        checkNotNull(filter);
        this.filter = filter;
        return this;
    }

    public FeatureReaderBuilder screenMap(@Nullable ScreenMap screenMap) {
        this.screenMap = screenMap;
        return this;
    }

    public FeatureReaderBuilder geometryFactory(@Nullable GeometryFactory geometryFactory) {
        this.geometryFactory = geometryFactory == null ? DEFAULT_GEOMETRY_FACTORY : geometryFactory;
        return this;
    }

    public FeatureReaderBuilder sortBy(@Nullable SortBy... sortBy) {
        this.sortBy = sortBy;
        return this;
    }

    public FeatureReaderBuilder offset(@Nullable Integer offset) {
        this.offset = offset;
        return this;
    }

    public FeatureReaderBuilder limit(@Nullable Integer limit) {
        this.limit = limit;
        return this;
    }

    public FeatureReader<SimpleFeatureType, SimpleFeature> build() {
        final String typeName = fullSchema.getTypeName();

        // query filter in native CRS
        final Filter nativeFilter = resolveNativeFilter();

        // properties needed by the output schema and the in-process filter, null means all
        // properties, empty list means no-properties needed
        final @Nullable Set<String> requiredProperties = resolveRequiredProperties(nativeFilter);
        // properties present in the RevTree nodes' extra data
        final Set<String> materializedProperties;
        // whether the RevTree nodes contain all required properties (hence no need to fetch
        // RevFeatures from the database)
        final boolean indexContainsAllRequiredProperties;
        // whether the filter is fully supported by the NodeRef filtering (hence no need for
        // pos-processing filtering). This is the case if the filter is a simple BBOX, Id, or
        // INCLUDE, or all the required properties are present in the index Nodes
        final boolean filterIsFullySupported;

        final ObjectId featureTypeId = typeRef.getMetadataId();

        // the RevTree id at the left side of the diff
        final ObjectId oldFeatureTypeTree;
        // the RevTree id at the right side of the diff
        final ObjectId newFeatureTypeTree;
        // where to get RevTree instances from (either the object or the index database)
        final ObjectStore treeSource;
        {

            // TODO: resolve based on filter, in case the feature type has more than one geometry
            // attribute
            final GeometryDescriptor geometryAttribute = fullSchema.getGeometryDescriptor();
            final Optional<Index> oldHeadIndex;
            final Optional<Index> headIndex;

            final Optional<NodeRef> oldCanonicalTree = resolveCanonicalTree(oldHeadRef, typeName);
            final Optional<NodeRef> newCanonicalTree = resolveCanonicalTree(headRef, typeName);
            final ObjectId oldCanonicalTreeId = oldCanonicalTree.isPresent()
                    ? oldCanonicalTree.get().getObjectId() : RevTree.EMPTY_TREE_ID;
            final ObjectId newCanonicalTreeId = newCanonicalTree.isPresent()
                    ? newCanonicalTree.get().getObjectId() : RevTree.EMPTY_TREE_ID;

            // featureTypeId = newCanonicalTree.isPresent() ? newCanonicalTree.get().getMetadataId()
            // : oldCanonicalTree.get().getMetadataId();

            Optional<Index> indexes[];

            // if native filter is a simple "fid filter" then force ignoring the index for a faster
            // look-up (looking up for a fid in the canonical tree is much faster)
            final boolean ignoreIndex = this.ignoreIndex || nativeFilter instanceof Id;
            if (ignoreIndex) {
                indexes = NO_INDEX;
            } else {
                indexes = resolveIndex(oldCanonicalTreeId, newCanonicalTreeId, typeName,
                        geometryAttribute.getLocalName());
            }
            oldHeadIndex = indexes[0];
            headIndex = indexes[1];
            // neither is present or both have the same indexinfo
            checkState(!(oldHeadIndex.isPresent() || headIndex.isPresent()) || //
                    headIndex.get().info().equals(oldHeadIndex.get().info()));

            if (oldHeadIndex.isPresent()) {
                oldFeatureTypeTree = oldHeadIndex.get().indexTreeId();
                newFeatureTypeTree = headIndex.get().indexTreeId();
            } else {
                oldFeatureTypeTree = oldCanonicalTreeId;
                newFeatureTypeTree = newCanonicalTreeId;
            }

            materializedProperties = resolveMaterializedProperties(headIndex);
            indexContainsAllRequiredProperties = materializedProperties
                    .containsAll(requiredProperties);
            filterIsFullySupported = filterIsFullySupported(nativeFilter,
                    indexContainsAllRequiredProperties);
            treeSource = headIndex.isPresent() ? repo.indexDatabase() : repo.objectDatabase();
        }

        // perform the diff op with the supported Bucket/NodeRef filtering that'll provide the
        // NodeRef iterator to back the FeatureReader with
        DiffTree diffOp = repo.command(DiffTree.class);
        // TODO: for some reason setting the default metadata id is making several tests fail,
        // though it's not really needed here because we have the FeatureType already. Nonetheless
        // this is strange and needs to be revisited.
        diffOp.setDefaultMetadataId(featureTypeId);
        diffOp.setPreserveIterationOrder(shallPreserveIterationOrder());
        diffOp.setPathFilter(resolveFidFilter(nativeFilter));
        diffOp.setCustomFilter(resolveNodeRefFilter());
        diffOp.setBoundsFilter(resolveBoundsFilter(nativeFilter, newFeatureTypeTree, treeSource));
        diffOp.setChangeTypeFilter(resolveChangeType());
        diffOp.setOldTree(oldFeatureTypeTree);
        diffOp.setNewTree(newFeatureTypeTree);
        diffOp.setLeftSource(treeSource);
        diffOp.setRightSource(treeSource);
        diffOp.recordStats();

        AutoCloseableIterator<DiffEntry> diffs;
        diffs = diffOp.call();
        {
            // Stopwatch sw = Stopwatch.createStarted();
            // int size = Iterators.size(diffs);
            // diffs.close();
            // System.err.printf("traversed %,d noderefs in %s\n", size, sw.stop());
            // diffs = diffOp.call();
        }
        AutoCloseableIterator<NodeRef> featureRefs = toFeatureRefs(diffs, changeType);

        // post-processing
        if (filterIsFullySupported) {
            featureRefs = applyOffsetAndLimit(featureRefs);
        }

        final ObjectStore featureSource = repo.objectDatabase();

        AutoCloseableIterator<SimpleFeature> features;

        // contains only the required attributes
        final SimpleFeatureType resultSchema = resolveOutputSchema(requiredProperties);

        final String strategy;
        if (indexContainsAllRequiredProperties) {
            strategy = MaterializedIndexFeatureIterator.class.getSimpleName();
            features = MaterializedIndexFeatureIterator.create(resultSchema, featureRefs,
                    geometryFactory);
        } else {
            strategy = BulkFeatureRetriever.class.getSimpleName();
            BulkFeatureRetriever retriever = new BulkFeatureRetriever(featureSource);
            features = retriever.getGeoToolsFeatures(featureRefs, fullSchema, geometryFactory);
        }

        if (!filterIsFullySupported) {
            FilterPredicate filterPredicate = new FilterPredicate(nativeFilter);
            features = AutoCloseableIterator.filter(features, filterPredicate);
            features = applyOffsetAndLimit(features);
        }

        features = loggingIterator(strategy, features, diffOp.getStats());

        FeatureReader<SimpleFeatureType, SimpleFeature> featureReader;

        featureReader = new FeatureReaderAdapter<SimpleFeatureType, SimpleFeature>(resultSchema,
                features);

        return featureReader;
    }

    private AutoCloseableIterator<SimpleFeature> loggingIterator(final String strategy,
            final AutoCloseableIterator<SimpleFeature> features, final Optional<Stats> stats) {

        return new AutoCloseableIterator<SimpleFeature>() {

            private final Stopwatch sw = Stopwatch.createStarted();

            @Override
            public SimpleFeature next() {
                return features.next();
            }

            @Override
            public boolean hasNext() {
                return features.hasNext();
            }

            @Override
            public void close() {
                sw.stop();
                String msg = strategy + ": " + sw.toString();
                if (stats.isPresent()) {
                    Stats s = stats.get();
                    msg += ", stats: " + s;
                }
                System.err.println(msg);
                features.close();
            }
        };
    }

    private SimpleFeatureType resolveOutputSchema(Set<String> requiredProperties) {
        SimpleFeatureType resultSchema;

        if (requiredProperties.equals(fullSchemaAttributeNames)) {
            resultSchema = fullSchema;
        } else {
            List<String> atts = new ArrayList<>();
            for (AttributeDescriptor d : fullSchema.getAttributeDescriptors()) {
                String attName = d.getLocalName();
                if (requiredProperties.contains(attName)) {
                    atts.add(attName);
                }
            }
            try {
                String[] properties = atts.toArray(new String[requiredProperties.size()]);
                resultSchema = DataUtilities.createSubType(fullSchema, properties);
            } catch (SchemaException e) {
                throw Throwables.propagate(e);
            }
        }

        return resultSchema;
    }

    private <T> AutoCloseableIterator<T> applyOffsetAndLimit(AutoCloseableIterator<T> iterator) {
        Integer offset = this.offset;
        Integer limit = this.limit;
        if (offset != null) {
            Iterators.advance(iterator, offset.intValue());
        }
        if (limit != null) {
            iterator = AutoCloseableIterator.limit(iterator, limit.intValue());
        }
        return iterator;
    }

    @SuppressWarnings("unchecked")
    private static final Optional<Index>[] NO_INDEX = new Optional[] { absent(), absent() };

    @SuppressWarnings("unchecked")
    private Optional<Index>[] resolveIndex(final ObjectId oldCanonical, final ObjectId newCanonical,
            final String treeName, final String attributeName) {
        if (Boolean.getBoolean("geogig.ignoreindex")) {
            // TODO: remove debugging aid
            System.err.printf(
                    "Ignoring index lookup for %s as indicated by -Dgeogig.ignoreindex=true\n",
                    treeName);
            return NO_INDEX;
        }

        Optional<Index>[] indexes = NO_INDEX;
        final IndexDatabase indexDatabase = repo.indexDatabase();
        Optional<IndexInfo> indexInfo = indexDatabase.getIndex(treeName, attributeName);
        if (indexInfo.isPresent()) {
            IndexInfo info = indexInfo.get();
            Optional<Index> oldIndex = resolveIndex(oldCanonical, info, indexDatabase);
            if (oldIndex.isPresent()) {
                Optional<Index> newIndex = resolveIndex(newCanonical, info, indexDatabase);
                if (newIndex.isPresent()) {
                    indexes = new Optional[2];
                    indexes[0] = oldIndex;
                    indexes[1] = newIndex;
                }
            }
        }
        return indexes;
    }

    private Optional<NodeRef> resolveCanonicalTree(@Nullable String head, String treeName) {
        Optional<NodeRef> treeRef = Optional.absent();
        if (head != null) {
            Optional<ObjectId> rootTree = repo.command(ResolveTreeish.class).setTreeish(head)
                    .call();
            if (rootTree.isPresent()) {
                RevTree tree = repo.objectDatabase().getTree(rootTree.get());
                treeRef = repo.command(FindTreeChild.class).setParent(tree).setChildPath(treeName)
                        .call();
            }
        }
        return treeRef;
    }

    private Optional<Index> resolveIndex(ObjectId canonicalTreeId, IndexInfo indexInfo,
            IndexDatabase indexDatabase) {

        Index index = new Index(indexInfo, RevTree.EMPTY_TREE_ID, indexDatabase);
        if (!RevTree.EMPTY_TREE_ID.equals(canonicalTreeId)) {
            Optional<ObjectId> indexedTree = indexDatabase.resolveIndexedTree(indexInfo,
                    canonicalTreeId);
            if (indexedTree.isPresent()) {
                index = new Index(indexInfo, indexedTree.get(), indexDatabase);
            }
        }
        return Optional.fromNullable(index);
    }

    private boolean filterIsFullySupported(Filter nativeFilter,
            boolean indexContainsAllRequiredProperties) {

        boolean filterSupported = Filter.INCLUDE.equals(nativeFilter) || //
                nativeFilter instanceof BBOX || //
                nativeFilter instanceof Id || //
                indexContainsAllRequiredProperties;

        return filterSupported;
    }

    private Set<String> resolveMaterializedProperties(Optional<Index> index) {
        Set<String> availableAtts = ImmutableSet.of();
        if (index.isPresent()) {
            IndexInfo info = index.get().info();
            availableAtts = IndexInfo.getMaterializedAttributeNames(info);
        }
        return availableAtts;
    }

    /**
     * Resolves the properties needed for the output schema, which are mandated both by the
     * properties requested by {@link #propertyNames} and any other property needed to evaluate the
     * {@link #filter} in-process.
     */
    private Set<String> resolveRequiredProperties(Filter nativeFilter) {
        if (outputSchemaPropertyNames == Query.ALL_NAMES) {
            return fullSchemaAttributeNames;
        }

        final Set<String> filterAttributes = requiredAttributes(nativeFilter);

        if (outputSchemaPropertyNames.length == 0
                /* Query.NO_NAMES */ && filterAttributes.isEmpty()) {
            return Collections.emptySet();
        }

        Set<String> requiredProps = Sets.newHashSet(outputSchemaPropertyNames);
        // if the filter is a simple BBOX filter against the default geometry attribute, don't force
        // it, we can optimize bbox filter out of Node.bounds()
        if (!(nativeFilter instanceof BBOX)) {
            requiredProps.addAll(filterAttributes);
        }
        // props required to evaluate the filter in-process
        return requiredProps;
    }

    private Set<String> requiredAttributes(Filter filter) {
        String[] filterAttributes = DataUtilities.attributeNames(filter);
        if (filterAttributes == null || filterAttributes.length == 0) {
            return Collections.emptySet();
        }

        return Sets.newHashSet(filterAttributes);
    }

    private AutoCloseableIterator<NodeRef> toFeatureRefs(
            final AutoCloseableIterator<DiffEntry> diffs, final ChangeType changeType) {

        return AutoCloseableIterator.transform(diffs, (e) -> {
            if (e.isAdd()) {
                return e.getNewObject();
            }
            if (e.isDelete()) {
                return e.getOldObject();
            }
            return ChangeType.CHANGED_OLD.equals(changeType) ? e.getOldObject() : e.getNewObject();
        });
    }

    private Filter resolveNativeFilter() {
        Filter nativeFilter = this.filter;
        nativeFilter = SimplifyingFilterVisitor.simplify(nativeFilter, fullSchema);
        nativeFilter = reprojectFilter(this.filter);
        return nativeFilter;
    }

    private Filter reprojectFilter(Filter filter) {
        if (hasSpatialFilter(filter)) {
            ReprojectingFilterVisitor visitor;
            visitor = new ReprojectingFilterVisitor(filterFactory, fullSchema);
            filter = (Filter) filter.accept(visitor, null);
        }
        return filter;
    }

    private boolean hasSpatialFilter(Filter filter) {
        SpatialFilterVisitor spatialFilterVisitor = new SpatialFilterVisitor();
        filter.accept(spatialFilterVisitor, null);
        return spatialFilterVisitor.hasSpatialFilter();
    }

    private org.locationtech.geogig.repository.DiffEntry.ChangeType resolveChangeType() {
        switch (changeType) {
        case ADDED:
            return DiffEntry.ChangeType.ADDED;
        case REMOVED:
            return DiffEntry.ChangeType.REMOVED;
        default:
            return DiffEntry.ChangeType.MODIFIED;
        }
    }

    private @Nullable ReferencedEnvelope resolveBoundsFilter(Filter filterInNativeCrs,
            ObjectId featureTypeTreeId, ObjectStore treeSource) {
        if (RevTree.EMPTY_TREE_ID.equals(featureTypeTreeId)) {
            return null;
        }

        CoordinateReferenceSystem nativeCrs = fullSchema.getCoordinateReferenceSystem();
        if (nativeCrs == null) {
            nativeCrs = DefaultEngineeringCRS.GENERIC_2D;
        }

        final Envelope queryBounds = new Envelope();
        List<Envelope> bounds = ExtractBounds.getBounds(filterInNativeCrs);

        if (bounds != null && !bounds.isEmpty()) {
            final RevTree tree = treeSource.getTree(featureTypeTreeId);
            final Envelope fullBounds = SpatialOps.boundsOf(tree);
            expandToInclude(queryBounds, bounds);

            Envelope clipped = fullBounds.intersection(queryBounds);
            if (clipped.equals(fullBounds)) {
                queryBounds.setToNull();
            }
        }
        return queryBounds.isNull() ? null : new ReferencedEnvelope(queryBounds, nativeCrs);
    }

    private void expandToInclude(Envelope queryBounds, List<Envelope> bounds) {
        for (Envelope e : bounds) {
            queryBounds.expandToInclude(e);
        }
    }

    private Predicate<Bounded> resolveNodeRefFilter() {
        Predicate<Bounded> predicate = Predicates.alwaysTrue();
        final boolean ignore = Boolean.getBoolean("geogig.ignorescreenmap");
        if (screenMap != null && !ignore) {
            predicate = new ScreenMapPredicate(screenMap);
        }
        return predicate;
    }

    private List<String> resolveFidFilter(Filter filter) {
        List<String> pathFilters = ImmutableList.of();
        if (filter instanceof Id) {
            final Set<Identifier> identifiers = ((Id) filter).getIdentifiers();
            Iterator<FeatureId> featureIds = Iterators
                    .filter(Iterators.filter(identifiers.iterator(), FeatureId.class), notNull());
            Preconditions.checkArgument(featureIds.hasNext(), "Empty Id filter");

            pathFilters = Lists.newArrayList(Iterators.transform(featureIds, (fid) -> fid.getID()));
        }

        return pathFilters;
    }

    /**
     * Determines if the returned iterator shall preserve iteration order among successive calls of
     * the same query.
     * <p>
     * This is the case if:
     * <ul>
     * <li>{@link #limit} and/or {@link #offset} have been set, since most probably the caller is
     * doing paging
     * </ul>
     */
    private boolean shallPreserveIterationOrder() {
        boolean preserveIterationOrder = false;
        preserveIterationOrder |= limit != null || offset != null;
        // TODO: revisit, sortBy should only force iteration order if we can return features in the
        // requested order, otherwise a higher level decorator will still perform the sort
        // in-process so there's no point in forcing the iteration order here
        // preserveIterationOrder |= sortBy != null && sortBy.length > 0;
        return preserveIterationOrder;
    }
}
