/* Copyright (c) 2013-2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Victor Olaya (Boundless) - initial implementation
 */
package org.locationtech.geogig.porcelain;

import static com.google.common.collect.Lists.newArrayList;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.Ref;
import org.locationtech.geogig.model.RevTag;
import org.locationtech.geogig.plumbing.ForEachRef;
import org.locationtech.geogig.repository.impl.AbstractGeoGigOp;

/**
 * Returns a list of all tags
 * 
 */
public class TagListOp extends AbstractGeoGigOp<List<RevTag>> {

    protected @Override List<RevTag> _call() {
        List<Ref> refs = newArrayList(
                command(ForEachRef.class).setPrefixFilter(Ref.TAGS_PREFIX).call());

        Stream<ObjectId> tagIds = refs.stream().map(Ref::getObjectId);

        try (Stream<RevTag> tags = objectDatabase().getAll(tagIds, RevTag.class)) {
            return tags.collect(Collectors.toList());
        }
    }
}
