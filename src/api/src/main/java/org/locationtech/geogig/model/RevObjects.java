/* Copyright (c) 2016 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.model;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Comparator;
import java.util.Iterator;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

/**
 * A set of utility methods to work with revision objects
 *
 * 
 * @since 1.0
 */
public class RevObjects {

    private static final char[] HEX_DIGITS = "0123456789abcdef".toCharArray();

    /**
     * Creates a hexadecimal encoding representation of the first {@code numBytes} bytes of the
     * SHA-1 hashcode represented by {@code id} and appends it to {@code target}.
     * 
     * @pre {@code 0 < numBytes <= 20 }
     * @param id the {@code ObjectId} representing the SHA-1 hash to encode as an hex string
     * @param numBytes
     * @param target the {@code StringBuilder} where to append the string representation of the
     *        {@link ObjectId}
     * @return {@code target}
     */
    public static StringBuilder toString(final ObjectId id, final int numBytes,
            StringBuilder target) {

        Preconditions.checkNotNull(id);
        Preconditions.checkArgument(numBytes > 0 && numBytes <= ObjectId.NUM_BYTES);

        StringBuilder sb = target == null ? new StringBuilder(2 * numBytes) : target;
        byte b;
        for (int i = 0; i < numBytes; i++) {
            b = (byte) id.byteN(i);
            sb.append(HEX_DIGITS[(b >> 4) & 0xf]).append(HEX_DIGITS[b & 0xf]);
        }
        return sb;
    }

    /**
     * Creates and returns an iterator of the joint lists of {@link RevTree#trees() trees} and
     * {@link RevTree#features() features} of the given {@code RevTree} whose iteration order is
     * given by the provided {@code comparator}.
     * 
     * @return an iterator over the <b>direct</b> {@link RevTree#trees() trees} and
     *         {@link RevTree#features() feature} children collections, in the order mandated by the
     *         provided {@code comparator}
     */
    public static Iterator<Node> children(RevTree tree, Comparator<Node> comparator) {
        checkNotNull(comparator);
        ImmutableList<Node> trees = tree.trees();
        ImmutableList<Node> features = tree.features();
        if (trees.isEmpty()) {
            return features.iterator();
        }
        if (features.isEmpty()) {
            return trees.iterator();
        }
        return Iterators.mergeSorted(ImmutableList.of(trees.iterator(), features.iterator()),
                comparator);
    }

}
