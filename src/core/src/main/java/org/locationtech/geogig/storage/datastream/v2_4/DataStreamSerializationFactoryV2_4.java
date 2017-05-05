/* Copyright (c) 2016 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.storage.datastream.v2_4;

import org.locationtech.geogig.storage.ObjectStore;
import org.locationtech.geogig.storage.datastream.DataStreamSerializationFactoryV2_1;

/**
 * Serialization factory for serial version 2.2
 *
 * @see FormatCommonV2_4
 */
public class DataStreamSerializationFactoryV2_4 extends DataStreamSerializationFactoryV2_1 {

    public DataStreamSerializationFactoryV2_4(ObjectStore source) {
        super(new FormatCommonV2_4(source));
    }

    @Override
    public String getDisplayName() {
        return "Binary 2.4";
    }

}