/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.storage.datastream.v2_4;

import org.junit.After;
import org.locationtech.geogig.storage.ObjectStore;
import org.locationtech.geogig.storage.impl.ObjectSerializationFactoryTest;
import org.locationtech.geogig.storage.impl.ObjectSerializingFactory;
import org.locationtech.geogig.storage.memory.HeapObjectStore;

public class DataStreamSerializationFactoryV2_4Test extends ObjectSerializationFactoryTest {

    ObjectStore source;

    public @After void sourceteardown() {
        if (source != null) {
            source.close();
        }
    }

    @Override
    protected ObjectSerializingFactory getObjectSerializingFactory() {
        if (source == null) {
            source = new HeapObjectStore();
            source.open();
        }
        return new DataStreamSerializationFactoryV2_4(source);
    }

}
